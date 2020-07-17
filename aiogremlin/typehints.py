from typing import *
import asyncio

# Common type-hints shared across multiple modules

Loop = asyncio.AbstractEventLoop
MaybeLoop = Optional[Loop]

MaybeStr = Optional[str]
MaybeInt = Optional[int]


