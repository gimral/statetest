package org.example.sideinput;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

import java.util.Map;

public class SideInputTest {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollectionView<Map<Long, Long>> map = p.apply(GenerateSequence.from(1).withRate(1, Duration.standardSeconds(4)))
                .apply(WithKeys.of(1L))
                .apply(ParDo.of(new SideInputDoFn()))
                .apply(Window.<KV<Long,Long>>into(new GlobalWindows())
                        .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                        .discardingFiredPanes())
                .apply(View.asMap());

        p.apply(GenerateSequence.from(1).withRate(2,Duration.standardSeconds(4)))
                .apply(ParDo.of(new DoFn<Long, Long>() {
                    @ProcessElement
                    public void processElement(ProcessContext c){
                        Map<Long,Long> lookup = c.sideInput(map);
                        if(lookup.containsKey(c.element())){
                            System.out.println("Found"+ c.element());
                            return;
                        }
                        System.out.println("Not Found"+ c.element());
                        c.output(c.element());
                    }
                }).withSideInputs(map));

        p.run().waitUntilFinish();

    }
}
