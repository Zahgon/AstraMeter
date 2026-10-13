import asyncio
import time
from collections.abc import Callable

from astrameter.powermeter.base import Powermeter, stream_fresh

from .base import PowermeterWrapper

#: How old the control loop's last reading may be before a passive reader
#: (the Marstek app responder, cloud reporting) reads the source itself.
#: Batteries poll about once a second, so while any battery reads this meter
#: a passive reader never adds a request of its own.
PASSIVE_READING_MAX_AGE_S = 5.0


class LastReadingPowermeter(PowermeterWrapper):
    """Innermost wrapper that lets passive readers reuse the control loop's read.

    Every real read of the source passes through here and is remembered.
    :meth:`get_passive_watts` — what the diagnostic readers call —
    serves that reading while it is at most ``max_age`` seconds old, and only
    reads the source itself when nothing has read it for longer, so those
    readers add no load on a meter the batteries are already polling.

    A push meter already answers from the reading it was sent, with no I/O, so
    its passive reads go straight through and keep the meter's own staleness
    rule.

    A passive read that finds no fresh reading first waits for a read of the
    source already in flight, a battery's or another passive reader's, and
    only reads the source itself if that one left nothing fresh behind.

    ``reset()`` passes through without forgetting the reading: it clears the
    filters' rolling state, and this is a fact about the meter.
    """

    def __init__(
        self,
        wrapped_powermeter: Powermeter,
        *,
        max_age: float = PASSIVE_READING_MAX_AGE_S,
        clock: Callable[[], float] | None = None,
    ) -> None:
        super().__init__(wrapped_powermeter)
        self.max_age = max_age
        self._clock = clock or time.monotonic
        self._values: list[float] | None = None
        self._read_at: float | None = None
        # Every read of the source still running: battery polls can overlap
        # when no throttle sits above. Each resolves to None however its read
        # ends, so a waiting passive reader looks at the cache afterwards
        # rather than at the read's outcome.
        self._in_flight: set[asyncio.Future[None]] = set()

    async def get_powermeter_watts(self) -> list[float]:
        done: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._in_flight.add(done)
        try:
            values = await self.wrapped_powermeter.get_powermeter_watts()
            if values:
                self._values = list(values)
                self._read_at = self._clock()
            return values
        finally:
            done.set_result(None)
            self._in_flight.discard(done)

    async def get_passive_watts(self) -> list[float]:
        if self.wrapped_powermeter.stream_online() is not None:
            return await self.wrapped_powermeter.get_passive_watts()
        while True:
            fresh = self._fresh_values()
            if fresh is not None:
                return fresh
            if not self._in_flight:
                return await self.get_powermeter_watts()
            # asyncio.wait never cancels what it waits on, even if this
            # reader is cancelled, so the reads it waits for run to the end.
            await asyncio.wait(
                set(self._in_flight), return_when=asyncio.FIRST_COMPLETED
            )

    def _fresh_values(self) -> list[float] | None:
        if self._values is not None and stream_fresh(
            self._read_at, self.max_age, self._clock
        ):
            return list(self._values)
        return None
