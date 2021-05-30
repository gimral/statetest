package org.example.sideinput;

import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SideInputDoFn extends DoFn<KV<Long,Long>, KV<Long,Long>> {
    @TimerId("timer")
    private final TimerSpec timerSpec;

    private static List<Long> initItems;

    private static boolean initialized;

    public static int getInitCount() {
        return initCount;
    }

    private static int initCount;

    public SideInputDoFn() {
        timerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);
        initItems = new ArrayList<>();
        initialized = false;
    }

    @ProcessElement
    public void processElement(ProcessContext c,@TimerId("timer") Timer timer){
        Long value = c.element().getValue();
        if(value>2000 || (value > 400 && value < 700)){
            return;
        }
        if(initialized) {
            //System.out.println("sideout sent " +  value);
            c.output(KV.of(value,value));
            return;
        }
        if(value<=300){
            //System.out.println("timer set " +  DateTime.now().toString());
            timer.offset(Duration.standardSeconds(3)).setRelative();
        }
        //System.out.println("sideout stored " +  value);
        initItems.add(value);
    }

    @OnTimer("timer")
    public void timerExpire(OnTimerContext c){
        System.out.println("timer fired " +  DateTime.now().toString());

        Instant timeStamp = DateTime.now().toInstant();
        for(Long value:initItems){
            c.outputWithTimestamp(KV.of(value,value),timeStamp);
            //c.output(KV.of(value,value));
        }
        initialized=true;
        initItems.clear();

    }
}
