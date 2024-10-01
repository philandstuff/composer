#!/usr/bin/env python

import asyncio

from graph import predict, value, transform, Composite


async def hello(x: str):
    return {"prompt": f"Hello, {x}!"}


if __name__ == "__main__":
    val = value("world")
    f = transform(hello)
    p = predict("black-forest-labs/flux-schnell")
    c = Composite()
    c.add(val)
    c.add(f, val)
    c.add(p, f)
    asyncio.run(c.execute())
