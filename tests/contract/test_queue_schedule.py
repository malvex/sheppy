from datetime import datetime

import pytest

from sheppy import Queue
from tests.dependencies import simple_async_task


class TestEdgeCases:
    async def test_offset_naive_datetime(self, queue: Queue):

        dt_without_tz = datetime.fromisoformat("2030-01-01 00:00:00")

        with pytest.raises(TypeError, match="provided datetime must be offset-aware"):
            await queue.schedule(simple_async_task(1, 2), at=dt_without_tz)
