#!/usr/bin/env python

import asyncio

from graph import predict, value, transform, Composite


async def prompt_hello(x: str):
    return {"prompt": f"Hello, {x}!"}


async def url_to_image(urls):
    return {"image": urls[0]}


async def combine_outputs(caption, url):
    return {"image": url, "caption": f"{caption}, but big"}


if __name__ == "__main__":
    world = value("world")
    greet = transform(prompt_hello)
    generate = predict("black-forest-labs/flux-schnell")
    upscale = predict(
        "nightmareai/real-esrgan:f121d640bd286e1fdc67f9799164c1d5be36ff74576ee11c803ae5b665dd46aa"
    )
    caption = predict(
        "salesforce/blip:2e1dddc8621f72155f24cf2e0adbde548458d3cab9f00c0139eea840d0ac4746"
    )
    image_input = transform(url_to_image)
    c = Composite()
    c.add(world)
    c.add(greet, world)
    c.add(generate, greet)
    c.add(image_input, generate)
    c.add(upscale, image_input)
    c.add(caption, image_input)
    c.add(transform(combine_outputs), caption, upscale)
    asyncio.run(c.execute())
