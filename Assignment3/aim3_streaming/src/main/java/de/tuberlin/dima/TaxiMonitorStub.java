package de.tuberlin.dima;

import java.util.*;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**
 * An example of grouped stream windowing where different eviction and trigger
 * policies can be used. A source fetches events from cars every 1 sec
 * containing their id, their current speed (km/h), overall elapsed distance (m)
 * and a timestamp. Your implementation should do the following:
 * 1. notifies, when a Taxi drives faster than 50 km/h
 * 2. notifies, when a Taxi stopped for a least 5 seconds.
 */
public class TaxiMonitorStub {

    private static final int NUM_CAR_EVENTS = 1000;

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        System.err.println("Usage: TaxiMonitor --mode <s(peed over 50) or p(arking)>");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        int numOfCars = 2;
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData = env.addSource(CarSource.create(numOfCars));

        // Set the mode to s or p.
        String mode = params.get("mode", "s");
        if (mode.equals("s")) {
            System.err.println("Mode = Speed");
        } else {
            mode = "p";
            System.err.println("Mode = Parking");
        }

        DataStream<Tuple4<Integer, Integer, Double, Long>> output = null;

        if (mode.equals("s")) {
            output = higherThan50(carData);
        } else {
            output = parkingSince5Seconds(carData);
        }

        output.print();
        env.setParallelism(1);
        env.execute("TaxiMonitor");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    /**
     * TODO Implement
     * Checks whether a car drives faster than 50.
     *
     * @param input The car data to process.
     * @return output    Tuples of cars that drive faster than 50.
     */
    public static DataStream<Tuple4<Integer, Integer, Double, Long>> higherThan50(
            DataStream<Tuple4<Integer, Integer, Double, Long>> input) {

        return input.filter(
                (FilterFunction<Tuple4<Integer, Integer, Double, Long>>) integerIntegerDoubleLongTuple4 -> integerIntegerDoubleLongTuple4.f1 > 50
        )
                ;
    }

    /**
     * TODO Implement
     * Checks whether a car parks for at least 5 seconds.
     * Hint: The timestamp may mark the time the car stopped.
     *
     * @param input The car data to process.
     * @return output    Tuples of cars that park since at least 5 seconds.
     */
    public static DataStream<Tuple4<Integer, Integer, Double, Long>> parkingSince5Seconds(
            DataStream<Tuple4<Integer, Integer, Double, Long>> input) {

        DataStream<Tuple4<Integer, Integer, Double, Long>> test = input.keyBy(0).flatMap(new ParkedCars());
        return test.assignTimestampsAndWatermarks(new CarTimestamp());
    }

    private static final HashMap<Integer, ArrayList<Long>> temp = new HashMap<>();

    public static boolean testMethod(Integer id, Integer speed, Long time) {

        if (speed == 0) {
            if (temp.containsKey(id)) {
                temp.get(id).add(time);
                return false;
            } else {
                ArrayList<Long> list = new ArrayList<>();
                list.add(time);
                temp.put(id, list);
                return false;
            }
        } else {
            if (temp.containsKey(id)) {
                temp.get(id).add(time);
                return timeCheck(temp, id);
            } else {
                return false;
            }
        }
    }

    private static boolean timeCheck(HashMap<Integer, ArrayList<Long>> state, Integer id) {
        ArrayList<Long> timeList = state.get(id);
        if ((timeList.get(timeList.size() - 2) - timeList.get(0)) >= 5000L) {
            System.out.println("Beginstate " + timeList.get(0));
            System.out.println("Endstate " + timeList.get(timeList.size() - 2));
            System.out.println("Elapsedtime " + (timeList.get(timeList.size() - 2) - timeList.get(0)));
            state.remove(id);
            return true;
        } else {
            return false;
        }

    }

    public static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distances;
        private Boolean[] stopped;

        private Random rand = new Random();

        private volatile boolean isRunning = true;
        private int counter;
        private int intervall;

        private CarSource(int numOfCars, int intervall) {
            this.intervall = intervall;
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            stopped = new Boolean[numOfCars];
            Arrays.fill(speeds, 25);
            Arrays.fill(distances, 0d);
            Arrays.fill(stopped, false); //cars drive at the beginning
        }

        public static CarSource create(int cars) {
            return new CarSource(cars, 100);
        }

        public static CarSource create(int cars, int intervall) {
            return new CarSource(cars, intervall);
        }

        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {

            /**
             * TODO Adjust, so that the generator fulfills the following requirements
             * 		The car should randomly stop driving for some time (at least once for 5 seconds)
             * 		The car should rarely drive faster than 50 km/h
             */
            int carStopId = -1;
            int carStopAmount = 0;
            while (isRunning && counter < NUM_CAR_EVENTS) {


                Thread.sleep(intervall); //don't touch the intervall, it will be 100 for you

                for (int carId = 0; carId < speeds.length; carId++) {

                    if (carId != carStopId) {

                        if (rand.nextBoolean()) {

                            if (rand.nextBoolean()) {
                                speeds[carId] = Math.min(65, speeds[carId] + 5);
                            } else {
                                speeds[carId] = Math.max(40, speeds[carId] - 5);
                            }
                            if (carStopId == -1) {
                                if (rand.nextBoolean()) {
                                    speeds[carId] = 0;
                                    carStopId = carId;
                                    carStopAmount = 50;
                                }
                            }
                        } else {
                            if (rand.nextBoolean()) {
                                speeds[carId] = Math.min(65, speeds[carId] + 5);
                            } else {
                                speeds[carId] = Math.max(0, speeds[carId] - 5);
                            }
                        }
                    } else {
                        speeds[carId] = 0;
                        carStopAmount--;
                        System.out.println("=====" + carStopAmount);
                        if (carStopAmount <= 0) {
                            carStopAmount = 0;
                            carStopId = -1;
                        }
                    }
                    distances[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
                            speeds[carId], distances[carId], System.currentTimeMillis());
                    ctx.collect(record);
                    counter++;

                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }


    }

    public static class ParkedCars extends RichFlatMapFunction<Tuple4<Integer, Integer, Double, Long>, Tuple4<Integer, Integer, Double, Long>> {

        private transient ValueState<Tuple1<HashMap>> states;


        public void flatMap(Tuple4<Integer, Integer, Double, Long> in, Collector<Tuple4<Integer, Integer, Double, Long>> collector) throws Exception {
            HashMap<Integer, List<Tuple4<Integer, Integer, Double, Long>>> newStates;
            if (states.value().f0 == null){
                newStates = new HashMap<>();
            } else {
                newStates = states.value().f0;
            }

            if (in.f1 != 0){
                if (newStates.containsKey(in.f0)){
                    newStates.remove(in.f0);
                }
            } else {
                if (newStates.containsKey(in.f0)){
                    newStates.get(in.f0).add(in);
                    checkNewAdded(in.f0, newStates, in, collector);
                } else {
                    ArrayList<Tuple4<Integer, Integer, Double, Long>> tuple4s = new ArrayList<>();
                    tuple4s.add(in);
                    newStates.put(in.f0, tuple4s);
                }
            }
            states.update(new Tuple1<>(newStates));
        }

        private void checkNewAdded(Integer id, HashMap<Integer, List<Tuple4<Integer, Integer, Double, Long>>> state, Tuple4<Integer, Integer, Double, Long> in, Collector<Tuple4<Integer, Integer, Double, Long>> collector){

            int length = state.get(id).size();
            List<Tuple4<Integer, Integer, Double, Long>> list = state.get(id);

            if ((list.get(length - 1).f3 - list.get(0).f3) >= 5000){
                System.out.println("Start node " + list.get(0));
                System.out.println("End   node " + list.get(length - 1));
                System.out.println("Time elaps " + (list.get(length - 1).f3 - list.get(0).f3));
                collector.collect(in);
            }
        }


        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple1<HashMap>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple1<HashMap>>() {
                            }), // type information
                            Tuple1.of(null)); // default value of the state, if nothing was set
            states = getRuntimeContext().getState(descriptor);

        }


    }


    public static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String record) {
            String rawData = record.substring(1, record.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
        }
    }

    public static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }

}
