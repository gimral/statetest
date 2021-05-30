package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import leap.data.beam.transforms.join.OneToOneJoin;

public class CoGroupByTest {
    static final TupleTag<String> leftTupleTag = new TupleTag<String>() {
    };
    static final TupleTag<Long> rightTupleTag = new TupleTag<Long>() {
    };

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        PCollection<KV<Long, String>> left = p.apply(GenerateSequence.from(0).withRate(2000, Duration.standardSeconds(1)))
                .apply("left", ParDo.of(new DoFn<Long, KV<Long, String>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c
                    ) {
                        if (c.element() < 1000000)
                            c.output(KV.of(c.element(), "l" + c.element().toString()));
                    }
                }));

        PCollection<KV<Long, Long>> right = p.apply(GenerateSequence.from(0).withRate(2000, Duration.standardSeconds(1)))
                .apply("right", ParDo.of(new DoFn<Long, KV<Long, Long>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c
                    ) {
                        if (c.element() < 500000)
                            c.output(KV.of(c.element(), c.element()));
                    }
                }));


        left = left.apply("Left Collection Global Window", Window.<KV<Long, String>>into(new GlobalWindows())
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO));

        right = right.apply("Right Collection Global Window", Window.<KV<Long, Long>>into(new GlobalWindows())
                .withTimestampCombiner(TimestampCombiner.EARLIEST)
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO));

        //PCollection<KV<Long, CoGbkResult>> coGroupByResult =
        PCollection<KV<Long, RawUnionValue>> result =
                KeyedPCollectionTuple.of(leftTupleTag, left)
                        .and(rightTupleTag, right)
                        .apply("CoGroupBy", CoGroupByKeyT.create());

        result.apply("State", ParDo.of(new DoFn<KV<Long, RawUnionValue>, KV<String, Long>>() {
                                           @StateId("leftstate")
                                           private final StateSpec<ValueState<String>> leftState = StateSpecs.value();
                                           @StateId("rightstate")
                                           private final StateSpec<ValueState<Long>> rightState = StateSpecs.value();
                                           @TimerId("gcTimer")
                                           private final TimerSpec leftStateExpiryTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);


                                           @ProcessElement
                                           public void process(
                                                   ProcessContext c,
                                                   @Timestamp Instant ts,
                                                   @AlwaysFetched @StateId("leftstate") ValueState<String> leftstate,
                                                   @AlwaysFetched @StateId("rightstate") ValueState<Long> rightstate,

                                                   @TimerId("gcTimer") Timer gcTimer
                                                   //@StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestamp,
                                           ) {

                                               //maxTimestamp.add(ts.getMillis());
                                               //state.write(element.getValue());
                                               // Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
                                               // as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
                                               // worth of event time (as measured by the watermark), then the gc timer will fire.
                                               Instant expirationTime = new Instant(ts.getMillis()).plus(Duration.standardSeconds(5));
                                               String left = leftstate.read();
                                               Long right = rightstate.read();
                                               RawUnionValue val = c.element().getValue();
                                               if(val.getUnionTag() == 0){
                                                   left = (String)val.getValue();
                                               }
                                               else if(val.getUnionTag() == 1){
                                                   right = (Long)val.getValue();
                                               }
                                               if(left !=  null && right != null){
                                                   leftstate.clear();
                                                   rightstate.clear();
                                                   c.output(KV.of(left,right));
                                                   return;
                                               }
                                               if(left == null){
                                                   rightstate.write(right);
                                               }
                                               else {
                                                   leftstate.write(left);
                                               }

                                               //state.write(c.element().getKey());
                                               //logger.info("Timer time", expirationTime);
                                               gcTimer.set(expirationTime);
                                               //state.clear();

                                           }

                                           @OnTimer("gcTimer")
                                           public void onLeftCollectionStateExpire(OnTimerContext c,
                                                                                   @StateId("leftstate") ValueState<String> leftstate,
                                                                                   @StateId("rightstate") ValueState<Long> rightstate
                                           ) {
                                               leftstate.clear();
                                               rightstate.clear();


                                           }
                                       }

        )).setCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()))
    .apply("print", ParDo.of(new DoFn<KV<String, Long>, String>() {
            @ProcessElement
            public void process(
                    ProcessContext c
            ) {
                System.out.println("l" + c.element().getKey()
                        + "r" + c.element().getValue());
                }
        }));

        //left.apply("join", OneToOneJoin.left(right)).droppedElementsIgnored();

        p.run().waitUntilFinish();
    }
}
