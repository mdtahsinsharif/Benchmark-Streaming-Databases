from pyflink.datastream import StreamExecutionEnvironment, SourceFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Types
from pyflink.common.time import Time
import time

# Python SourceFunction
class SimpleSource(SourceFunction):

    def run(self, ctx):
        data = [(1, 10), (1, 20), (2, 5), (1, 5), (2, 15)]
        for item in data:
            ctx.collect(item)
            time.sleep(1)

    def cancel(self):
        pass

# Simple ProcessWindowFunction
class WindowStats(ProcessWindowFunction):
    def process(self, key, context, elements, out):
        total = sum(e[1] for e in elements)
        out.collect((key, total))

# Create environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Add source properly
ds = env.add_source(SimpleSource(), type_info=Types.TUPLE([Types.INT(), Types.INT()]))

# Key by first element and sum in a 5-second window
ds.key_by(lambda x: x[0]) \
  .window(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
  .process(WindowStats(), output_type=Types.TUPLE([Types.INT(), Types.INT()])) \
  .print()

env.execute("Test Flink Window Job")
