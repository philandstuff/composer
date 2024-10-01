#!/usr/bin/env python

import asyncio

from graph import predict, value, transform, Composite


async def hello(x: str):
    return f"Hello, {x}!"


if __name__ == "__main__":
    val = value("world")
    f = transform(hello)
    p = predict("foo/bar")
    c = Composite()
    c.add(val)
    c.add(f, val)
    c.add(p, f)
    asyncio.run(c.execute())
