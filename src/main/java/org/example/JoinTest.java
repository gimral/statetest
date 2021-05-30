package org.example;

import leap.data.beam.transforms.join.OneToOneJoin;
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
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class JoinTest {
    static final TupleTag<String> leftTupleTag = new TupleTag<String>() {
    };
    static final TupleTag<Long> rightTupleTag = new TupleTag<Long>() {
    };

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
        Pipeline p = Pipeline.create(options);
        PCollection<KV<Long, String>> left = p.apply(GenerateSequence.from(0).withRate(10000, Duration.standardSeconds(1)))
                .apply("left", ParDo.of(new DoFn<Long, KV<Long, String>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c
                    ) {
                        if (c.element() < 600000)
                            c.output(KV.of(c.element(),  c.element().toString()));
                    }
                }));

        PCollection<KV<Long, Long>> right = p.apply(GenerateSequence.from(0).withRate(20000, Duration.standardSeconds(3)))
                .apply("right", ParDo.of(new DoFn<Long, KV<Long, Long>>() {
                    @ProcessElement
                    public void process(
                            ProcessContext c
                    ) {
                        if (c.element() < 500000)
                            c.output(KV.of(c.element(), c.element()));
                    }
                }));

//        left.apply("Print Left",ParDo.of(new DoFn<KV<Long, String>, String>() {
//            @ProcessElement
//            public void process(
//                    ProcessContext c
//            ) {
//                if(c.element().getKey() % 10000 == 0) {
//                    try {
//                        System.out.println("left-" + InetAddress.getLocalHost().getHostName() + "-" + c.element().getValue());
//                    } catch (UnknownHostException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }));

    left.apply("Join", OneToOneJoin.left(right))
            .droppedElementsIgnored()
    .apply("print", ParDo.of(new DoFn<KV<Long,KV<String, Long>>, String>() {
            @ProcessElement
            public void process(
                    ProcessContext c
            ) {
                if(c.element().getKey() % 10000 == 0) {
                    try {
                        System.out.println(InetAddress.getLocalHost().getHostName() + "-" + c.element().getValue().getKey()
                            + "-" + c.element().getValue().getValue());
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
        }));

        //left.apply("join", OneToOneJoin.left(right)).droppedElementsIgnored();

        p.run().waitUntilFinish();
    }
}
