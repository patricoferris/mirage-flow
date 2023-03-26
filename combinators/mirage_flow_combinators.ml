(*
 * Copyright (c) 2011-present Anil Madhavapeddy <anil@recoil.org>
 * Copyright (c) 2013-present Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (C) 2016-present David Scott <dave.scott@docker.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "mirage-flow-combinators"
module Log = (val Logs.src_log src : Logs.LOG)

module type CONCRETE =  Mirage_flow.S
  with type error = [ `Msg of string ]
   and type write_error = [ Mirage_flow.write_error | `Msg of string ]

module Concrete (S: Mirage_flow.S) = struct
  type error = [`Msg of string]
  type write_error = [ Mirage_flow.write_error | `Msg of string]
  type flow = S.flow

  let pp_error ppf = function
    | `Msg s -> Fmt.string ppf s

  let pp_write_error ppf = function
    | #error as e -> pp_error ppf e
    | `Closed     -> Mirage_flow.pp_write_error ppf `Closed

  let lift_read = function
    | Ok x    -> Ok x
    | Error e -> Error (`Msg (Fmt.str "%a" S.pp_error e))

  let lift_write = function
    | Ok ()         -> Ok ()
    | Error `Closed -> Error `Closed
    | Error e       -> Error (`Msg (Fmt.str "%a" S.pp_write_error e))

  let read t = S.read t |> lift_read
  let write t b = S.write t b |> lift_write
  let writev t bs = S.writev t bs |> lift_write
  let close t = S.close t
end

module type SHUTDOWNABLE = sig
  include Mirage_flow.S
  val shutdown_write: flow -> unit
  val shutdown_read: flow -> unit
end

type time = int64

type 'a stats = {
  read_bytes: int64 ref;
  read_ops: int64 ref;
  write_bytes: int64 ref;
  write_ops: int64 ref;
  finish: time option ref;
  start: time;
  time: unit -> time;
  t: (unit, 'a) result Eio.Promise.or_exn;
}

let stats t =
  let duration : int64 = match !(t.finish) with
    | None -> Int64.sub (t.time ()) t.start
    | Some x -> Int64.sub x t.start
  in {
    Mirage_flow.read_bytes = !(t.read_bytes);
    read_ops               = !(t.read_ops);
    write_bytes            = !(t.write_bytes);
    write_ops              = !(t.write_ops);
    duration;
  }

module Copy = struct
  open Eio

  type error = [ `A of exn | `B of exn ]

  let pp_error ppf = function
    | `A exn -> Fmt.exn ppf exn
    | `B exn -> Fmt.exn ppf exn

  let start ~sw ~mono (a: <Flow.two_way; read : Cstruct.t>) (b: <Flow.two_way; read : Cstruct.t>) =
    let read_bytes = ref 0L in
    let read_ops = ref 0L in
    let write_bytes = ref 0L in
    let write_ops = ref 0L in
    let finish = ref None in
    let now_in_ns () = Eio.Time.Mono.now mono |> Mtime.to_uint64_ns in
    let start = now_in_ns () in
    let rec loop () =
      match a#read with
      | exception End_of_file ->
        finish := Some (now_in_ns ());
        Ok ()
      | exception e ->
        finish := Some (now_in_ns ());
        Error (`A e)
      | buffer ->
        read_ops := Int64.succ !read_ops;
        read_bytes := Int64.(add !read_bytes (of_int @@ Cstruct.length buffer));
        match Flow.write b [buffer] with
        | () ->
          write_ops := Int64.succ !write_ops;
          write_bytes := Int64.(add !write_bytes (of_int @@ Cstruct.length buffer));
          loop ()
        | exception e ->
          finish := Some (now_in_ns ());
          Error (`B e)
    in
    {
      read_bytes;
      read_ops;
      write_bytes;
      write_ops;
      finish;
      start;
      time = (fun () -> now_in_ns ());
      t = (Eio.Fiber.fork_promise ~sw loop);
    }

  let wait t = Eio.Promise.await_exn t.t

  let copy ~mono ~src:a ~dst:b =
    Eio.Switch.run @@ fun sw ->
    let t = start ~sw ~mono a b in
    match wait t with
    | Ok ()   -> Ok (stats t)
    | Error e -> Error e

end

module Proxy =
struct

  type error = [
    | `A of Copy.error
    | `B of Copy.error
    | `A_and_B of (Copy.error * Copy.error)
  ]

  let pp_error ppf = function
    | `A_and_B (e1, e2) ->
      Fmt.pf ppf "flow proxy a: %a; flow proxy b: %a"
        Copy.pp_error e1 Copy.pp_error e2
    | `A e -> Fmt.pf ppf "flow proxy a: %a" Copy.pp_error e
    | `B e -> Fmt.pf ppf "flow proxy b: %a" Copy.pp_error e


  let proxy ~mono a b =
    Eio.Switch.run @@ fun sw ->
    let a2b =
      Eio.Fiber.fork_promise ~sw @@ fun () ->
      let t = Copy.start ~sw ~mono a b in
      let result = Copy.wait t in
      Eio.Flow.shutdown a `Receive;
      Eio.Flow.shutdown b `Send;
      let stats = stats t in
      match result with
      | Ok ()   -> Ok stats
      | Error e -> Error e
    in
    let b2a =
      Eio.Fiber.fork_promise ~sw @@ fun () ->
      let t = Copy.start ~sw ~mono b a in
      let result = Copy.wait t in
      Eio.Flow.shutdown b `Receive;
      Eio.Flow.shutdown a `Send;
      let stats = stats t in
      match result with
      | Ok ()   -> Ok stats
      | Error e -> Error e
    in
    let a_stats = Eio.Promise.await_exn a2b in
    let b_stats = Eio.Promise.await_exn b2a in
    match a_stats, b_stats with
    | Ok a_stats, Ok b_stats -> Ok (a_stats, b_stats)
    | Error e1  , Error e2   -> Error (`A_and_B (e1, e2))
    | Error e1  ,  _         -> Error (`A e1)
    | _         , Error e2   -> Error (`B e2)

end

module F = struct

  type refill = Cstruct.t -> int -> int -> int

  type error
  let pp_error ppf (_:error) =
    Fmt.string ppf "Mirage_flow_combinators.F.error"
  type write_error = Mirage_flow.write_error
  let pp_write_error = Mirage_flow.pp_write_error

  let seq f1 f2 buf off len =
    match f1 buf off len with
    | 0 -> f2 buf off len
    | n -> n

  let zero _buf _off _len = 0

  let rec iter fn = function
    | []   -> zero
    | h::t -> seq (fn h) (iter fn t)

  type flow = {
    close: unit -> unit;
    input: refill;
    output: refill;
    mutable buf: Cstruct.t;
    mutable ic_closed: bool;
    mutable oc_closed: bool;
  }

  let default_buffer_size = 4096

  let make ?(close=fun () -> ()) ?input ?output () =
    let buf = Cstruct.create default_buffer_size in
    let ic_closed = input = None in
    let oc_closed = output = None in
    let input = match input with None -> zero | Some x -> x in
    let output = match output with None -> zero | Some x -> x in
    { close; input; output; buf; ic_closed; oc_closed; }

  let input_fn len blit str =
    let str_off = ref 0 in
    let str_len = len str in
    fun buf off len ->
      if !str_off >= str_len then 0
      else (
        let len = min (str_len - !str_off) len in
        blit str !str_off buf off len;
        str_off := !str_off + len;
        len
      )

  let output_fn len blit str =
    let str_off = ref 0 in
    let str_len = len str in
    fun buf off len ->
      if !str_off >= str_len then 0
      else (
        let len = min (str_len - !str_off) len in
        blit buf off str !str_off len;
        str_off := !str_off + len;
        len
      )

  let mk fn_i fn_o ?input ?output () =
    let input = match input with None -> None | Some x -> Some (fn_i x) in
    let output = match output with None -> None | Some x -> Some (fn_o x) in
    make ?input ?output ()

  let input_string = input_fn String.length Cstruct.blit_from_string
  let output_bytes = output_fn Bytes.length Cstruct.blit_to_bytes
  let string = mk input_string output_bytes

  let input_cstruct = input_fn Cstruct.length Cstruct.blit
  let output_cstruct = output_fn Cstruct.length Cstruct.blit
  let cstruct = mk input_cstruct output_cstruct

  let input_strings = iter input_string
  let output_bytess = iter output_bytes
  let strings = mk input_strings output_bytess

  let input_cstructs = iter input_cstruct
  let output_cstructs = iter output_cstruct
  let cstructs = mk input_cstructs output_cstructs

  let refill ch =
    if Cstruct.length ch.buf = 0 then (
      let buf = Cstruct.create default_buffer_size in
      ch.buf <- buf
    )

  let read ch =
    if ch.ic_closed then Ok `Eof
    else (
      refill ch;
      let n = ch.input ch.buf 0 default_buffer_size in
      if n = 0 then (
        ch.ic_closed <- true;
        Ok `Eof;
      ) else (
        let ret = Cstruct.sub ch.buf 0 n in
        let buf = Cstruct.shift ch.buf n in
        ch.buf <- buf;
        Ok (`Data ret)
      )
    )

  let write ch buf =
    if ch.oc_closed then Error `Closed
    else (
      let len = Cstruct.length buf in
      let rec aux off =
        if off = len then Ok ()
        else (
          let n = ch.output buf off (len - off) in
          if n = 0 then (
            ch.oc_closed <- true;
            Error `Closed
          ) else aux (off+n)
        )
      in
      aux 0
    )

  let writev ch bufs =
    if ch.oc_closed then Error `Closed
    else
      let rec aux = function
        | []   -> Ok ()
        | h::t ->
          match write ch h with
          | Error e -> Error e
          | Ok ()   -> aux t
      in
      aux bufs

  let close ch =
    ch.ic_closed <- true;
    ch.oc_closed <- true;
    ch.close ()

end

type error = [`Msg of string]
type write_error = [ Mirage_flow.write_error | error ]
let pp_error ppf (`Msg s) = Fmt.string ppf s

let pp_write_error ppf = function
  | #Mirage_flow.write_error as e -> Mirage_flow.pp_write_error ppf e
  | #error as e                   -> pp_error ppf e

type flow =
  | Flow: string * (module CONCRETE with type flow = 'a) * 'a -> flow

type t = flow

let create (type a) (module M: Mirage_flow.S with type flow = a) t name =
  let m = (module Concrete(M): CONCRETE with type flow = a) in
  Flow (name, m , t)

let read (Flow (_, (module F), flow)) = F.read flow
let write (Flow (_, (module F), flow)) b = F.write flow b
let writev (Flow (_, (module F), flow)) b = F.writev flow b
let close (Flow (_, (module F), flow)) = F.close flow
let pp ppf (Flow (name, _, _)) = Fmt.string ppf name

let forward ?(verbose=false) ~src ~dst () =
  let rec loop () =
    match read src with
    | Ok `Eof ->
      Log.err (fun l -> l "forward[%a => %a] EOF" pp src pp dst)
    | Error e ->
      Log.err (fun l -> l "forward[%a => %a] %a" pp src pp dst pp_error e)
    | Ok (`Data buf) ->
      Log.debug (fun l ->
          let payload =
            if verbose then Fmt.str "[%S]" @@ Cstruct.to_string buf
            else Fmt.str "%d bytes" (Cstruct.length buf)
          in
          l "forward[%a => %a] %s" pp src pp dst payload);
      match write dst buf with
      | Ok ()   -> loop ()
      | Error e ->
        Log.err (fun l -> l "forward[%a => %a] %a"
                    pp src pp dst pp_write_error e)
  in
  loop ()

let proxy ?verbose f1 f2 =
  Eio.Fiber.all [
    forward ?verbose ~src:f1 ~dst:f2;
    forward ?verbose ~src:f2 ~dst:f1;
  ]
