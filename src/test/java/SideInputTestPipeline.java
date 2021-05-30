import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.example.sideinput.SideInputDoFn;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

public class SideInputTestPipeline {
    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void test() {
        PCollectionView<Map<Long, Long>> map = p.apply("SideInput Sequence",GenerateSequence.from(1).withRate(100, Duration.standardSeconds(1)))
                .apply("KeySideInputs",WithKeys.of(1L))
                .apply("Initialize Side Inputs",ParDo.of(new SideInputDoFn()))
                .apply("Window Side Inputs",Window.<KV<Long,Long>>into(new GlobalWindows())
//                        .triggering(Repeatedly.forever(
//                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(5))
//                                ))
                        .triggering(Repeatedly.forever(
                                AfterPane.elementCountAtLeast(1)
                        ))
                        .accumulatingFiredPanes())
                .apply(View.asMap());

        p.apply("MainInput Sequence",GenerateSequence.from(1).withRate(10,Duration.standardSeconds(1)))
                //.apply("Wait", ParDo.of(getWaitDoFn()))
                .apply("Lookup",ParDo.of(getLookupDoFn(map)).withSideInputs(map));

        p.run().waitUntilFinish();
    }

    private static DoFn<Long, Long> getLookupDoFn(PCollectionView<Map<Long, Long>> map) {
        return new DoFn<Long, Long>() {
            @ProcessElement
            public void processElement(ProcessContext c) throws InterruptedException {
                Map<Long,Long> lookup = c.sideInput(map);
                System.out.println("Lookup Size"+ lookup.size());
                if(lookup.containsKey(c.element())){
                    System.out.println("Found"+ c.element());
                    return;
                }
                System.out.println("Not Found"+ c.element());
                c.output(c.element());
            }
        };
    }

//    private static DoFn<Long, Long> getWaitDoFn() {
//        return new DoFn<Long, Long>() {
//            @ProcessElement
//            public void processElement(ProcessContext c) throws InterruptedException {
//                while(!SideInputDoFn.isInitialized())
//                    Thread.sleep(1000);
//                c.output(c.element());
//            }
//        };
//    }
}
