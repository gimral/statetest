package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StateTest {
    static final Logger logger =LoggerFactory.getLogger("main");
    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        logger.info("Starting");
        p.apply(GenerateSequence.from(0).withRate(20, Duration.standardSeconds(1)))
                .apply(WithKeys.of((val) -> val)).setCoder(KvCoder.of(VarLongCoder.of(),VarLongCoder.of()))
                .apply("Window", Window.<KV<Long, Long>>into(new GlobalWindows())
                //.apply("Window", Window.<KV<Long, Long>>into(FixedWindows.of(Duration.standardSeconds(2)))
                //.apply("Window", Window.<KV<Long, Long>>into(Sessions.withGapDuration(Duration.standardSeconds(5)))
                        .triggering(AfterPane.elementCountAtLeast(1))
                                //.triggering(AfterWatermark.pastEndOfWindow())
                        .discardingFiredPanes()
                        .withAllowedLateness(Duration.ZERO))
                .apply("State", ParDo.of(new DoFn<KV<Long, Long>, KV<Long, Long>>() {
                    @StateId("state")
                    private final StateSpec<ValueState<Long>> leftState = StateSpecs.value();
                    @TimerId("gcTimer")
                    private final TimerSpec leftStateExpiryTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);


                    @ProcessElement
                    public void process(
                            ProcessContext c,
                            @Timestamp Instant ts,
                            @StateId("state") ValueState<Long> state,
                            @TimerId("gcTimer") Timer gcTimer
                            //@StateId("maxTimestampSeen") CombiningState<Long, long[], Long> maxTimestamp,
                            ) {

                        //maxTimestamp.add(ts.getMillis());
                        //state.write(element.getValue());
                        // Set the timer to be one hour after the maximum timestamp seen. This will keep overwriting the same timer, so
                        // as long as there is activity on this key the state will stay active. Once the key goes inactive for one hour's
                        // worth of event time (as measured by the watermark), then the gc timer will fire.
                        Instant expirationTime = new Instant(ts.getMillis()).plus(Duration.standardSeconds(5));
                        c.output(c.element());
                        Long k = state.read();
                        if(k==null)
                            k = 0L;
                        state.write(++k);
                        //state.write(c.element().getKey());
                        //logger.info("Timer time", expirationTime);
                        gcTimer.set(expirationTime);
                        //state.clear();

                    }

                    @OnTimer("gcTimer")
                    public void onLeftCollectionStateExpire(OnTimerContext c,
                                                            @StateId("state") ValueState<Long> state
                    ) {
                        //logger.info("Timer Expire");
                        Long k = state.read();
                        //logger.info("{}",k);
//                        if(k % 200 == 0){
//                            logger.info("{}",k);
//                        }
                        state.clear();

                    }
                }

                )).setCoder(KvCoder.of(VarLongCoder.of(),VarLongCoder.of()))
        .apply("Log",ParDo.of(new DoFn<KV<Long, Long>, Void>() {
            @ProcessElement
            public void process(@Element KV<Long, Long> element){
                //logger.info("{}",element);
            }
        }));
        p.run().waitUntilFinish();

    }
}
