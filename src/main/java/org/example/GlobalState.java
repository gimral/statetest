package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class GlobalState {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        p.apply(GenerateSequence.from(0).withRate(10000, Duration.standardSeconds(1)))
                .apply(Filter.by((val) -> val < 1000000 || val % 500 == 0))
                .apply(WithKeys.of((val) -> val)).setCoder(KvCoder.of(VarLongCoder.of(),VarLongCoder.of()))
                .apply("Fixed Window",Window.into(FixedWindows.of(Duration.standardSeconds(5))))
                .apply("GroupBy", GroupByKey.create())
                .apply("Window", Window.into(new GlobalWindows())
                        //.triggering(AfterPane.elementCountAtLeast(1))
                        //.discardingFiredPanes()
                        //.withAllowedLateness(Duration.ZERO))
                )
                .apply("State", ParDo.of(new DoFn<KV<Long, Iterable<Long>>, KV<Long, Long>>() {
                                             @StateId("state")
                                             private final StateSpec<ValueState<Long>> stateSpec = StateSpecs.value();
                                             @TimerId("gcTimer")
                                             private final TimerSpec gcTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

                                             @ProcessElement
                                             public void process(
                                                     ProcessContext c,
                                                     @Timestamp Instant ts,
                                                     @StateId("state") ValueState<Long> state,
                                                     @TimerId("gcTimer") Timer gcTimer
                                             ) {
                                                 state.write(c.element().getKey());
                                                 Instant expirationTime = new Instant(ts.getMillis()).plus(Duration.standardSeconds(5));
                                                 gcTimer.set(expirationTime);
                                             }

                                             @OnTimer("gcTimer")
                                             public void onGC(@StateId("state") ValueState<Long> state ) {
                                                 state.clear();
                                             }
                                         }

                ));

        p.run().waitUntilFinish();
    }
}
