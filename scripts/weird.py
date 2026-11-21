#!/usr/bin/env python3
"""Derive a stress dataset whose IDs exercise the hashed-key paths: weird.py SRC DST.
User, event and click IDs are renamed consistently (long, spaced, punctuated variants)."""
import hashlib, os, shutil, sys

src, dst = sys.argv[1], sys.argv[2]
shutil.rmtree(dst, ignore_errors=True)
shutil.copytree(src, dst)

def f(v: bytes, salt: bytes) -> bytes:
    h = hashlib.blake2b(v + salt, digest_size=2).digest()[0] % 5
    if h == 0: return v + b"-long-suffix-" + v[::-1]   # long: hashed
    if h == 1: return b"~" + v                          # sorts after letters, hashed
    if h == 2: return b"!" + v                          # sorts before digits, hashed
    return v                                            # packed

def tr(field: bytes, salt: bytes) -> bytes:
    if len(field) >= 2 and field[:1] == b'"' and field[-1:] == b'"' and b'"' not in field[1:-1]:
        inner = field[1:-1]
        return b'"' + (f(inner, salt) if inner else inner) + b'"'
    if b'"' in field or not field or field.endswith(b"\r"):
        return field
    return f(field, salt)

cols = {"events": {0: b"e", 1: b"u"}, "clicks": {0: b"k", 1: b"u"}, "links": {0: b"u", 1: b"u"}}
for folder, which in cols.items():
    d = os.path.join(dst, folder)
    for name in os.listdir(d):
        p = os.path.join(d, name)
        data = open(p, "rb").read().split(b"\n")
        out = [data[0]]
        for line in data[1:]:
            parts = line.split(b",")
            for i, salt in which.items():
                if i < len(parts):
                    parts[i] = tr(parts[i], salt)
            out.append(b",".join(parts))
        open(p, "wb").write(b"\n".join(out))
print("ok")
