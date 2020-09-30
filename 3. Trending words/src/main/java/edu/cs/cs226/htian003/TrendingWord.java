
package edu.cs.cs226.htian003;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.Math.floor;

public class TrendingWord {
    public static void main(String[] args) {
        JavaSparkContext sc =
                new JavaSparkContext();
        String inputFileDir = args[0];
        JavaRDD<String> textFileRDDWithHeader = sc.textFile(inputFileDir + "/0.data");
        final String header = textFileRDDWithHeader.first();
        System.out.println("header is :" + header);
        //Remove the header
        JavaRDD<String> textFileRDD = textFileRDDWithHeader.filter(row -> !row.equalsIgnoreCase(header));
        // Create (timestamp, oneLineofText) pair
        JavaPairRDD<String, String> timeStampLinePair =
                textFileRDD.mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) {
                        String timeStamp = s.split("\t")[0];
                        return new Tuple2<String, String>(timeStamp, s);
                    }
                });

        // Resample time stamp by hour
        JavaPairRDD<Long, String> byHourLinePair = timeStampLinePair.mapToPair(resampleToHour);


        //Collect word by time
        JavaPairRDD<Tuple2<Long, String>, String> wordPair = byHourLinePair.flatMapToPair(extractWord);

        //Format the value as     time,textline,         word,   1
        JavaPairRDD<Tuple2<Long, String>, Tuple2<String, Integer>> wordCountPair
                = wordPair.mapValues(value -> new Tuple2<String, Integer>(value, 1));
        //                 time, State, word
        JavaPairRDD<Tuple3<Long, String, String>, Integer> wordCountByTimeState = wordCountPair.mapToPair(extractState);
        //Filter
        Map<String,String> stateMap = TrendingWord.STATE_MAP;
        JavaPairRDD<Tuple3<Long, String, String>, Integer>wordCountByTimeStateFiltered =
                wordCountByTimeState.filter(s->(stateMap.containsKey(s._1()._2())));
        //Reduce by key    time, state, word i.e, word count at same time, same state
        JavaPairRDD<Tuple3<Long, String, String>, Integer> reducedWordCount = wordCountByTimeStateFiltered.reduceByKey((a, b) -> a + b);
        //Reformat         state , word          time, count and Group by  (word, location)
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Long, Integer>>> useWordStateAsKey = reducedWordCount.mapToPair(throwTimeToValue).groupByKey();
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Long, Integer>>> continuousTime = useWordStateAsKey.mapToPair(makeTimeContinuous);
        JavaPairRDD<Tuple2<String, String>, Iterable<Tuple2<Long, Double>>> wordCountGradient = continuousTime.mapToPair(takeGradient);
        JavaPairRDD<Tuple2<String, String>, Tuple2<Long,Double>> gradientInRowFormat = wordCountGradient.flatMapToPair(formatToRows);
        JavaRDD<String> gradientAsLine = gradientInRowFormat.map(formatToLine);

        gradientAsLine.coalesce(1, true).saveAsTextFile(args[0] + "/output.txt");


        /*Data for Yang*/
/*      JavaPairRDD<Tuple2<String, String>, Tuple2<Long,Integer>> wcInRowFormat = useWordStateAsKey.flatMapToPair(formatToRows1);
        JavaRDD<String> wcAsLine = wcInRowFormat.map(formatToLine1);
        wcAsLine.coalesce(1, true).saveAsTextFile(args[1] + "/test3.txt");*/



    }

    private static PairFunction<Tuple2<String, String>, Long, String> resampleToHour = (tuple) -> {
        int t = 3600;
        DateFormat dateFormat = new SimpleDateFormat("MMM dd kk:mm:ss Z yyyy");
        Date date = dateFormat.parse(tuple._1());
        long unixTime = date.getTime() / 1000;
        long hourTime = (long) floor(unixTime / t);
        return new Tuple2<Long, String>(hourTime, tuple._2());
    };

    private static PairFlatMapFunction<Tuple2<Long, String>, Tuple2<Long, String>, String> extractWord = (tuple) -> {
        List<Tuple2<Tuple2<Long, String>, String>> list = new ArrayList<Tuple2<Tuple2<Long, String>, String>>();
        String wordColumn = tuple._2.split("\t")[2];
        String wordList[] = wordColumn.split(",");
        for (String token : wordList) {
            list.add(new Tuple2<Tuple2<Long, String>, String>(tuple, token));
        }
        return list.iterator();
    };
    //                                                                                     time, state,word     1
    private static PairFunction<Tuple2<Tuple2<Long, String>, Tuple2<String, Integer>>, Tuple3<Long, String, String>, Integer> extractState = (tuple) -> {
        String stateColumn = tuple._1._2.split("\t")[4];
        String state = stateColumn.split(", ")[stateColumn.split(", ").length - 1];
        Tuple3<Long, String, String> key = new Tuple3<>(tuple._1._1, state, tuple._2._1);
        return new Tuple2<Tuple3<Long, String, String>, Integer>(key, tuple._2._2);
    };

    private static PairFunction<Tuple2<Tuple3<Long, String, String>, Integer>, Tuple2<String,String>, Tuple2<Long,Integer>> throwTimeToValue = (tuple) -> {
        Tuple2<String,String> key= new Tuple2<>(tuple._1()._2(),tuple._1()._3());
        Tuple2<Long,Integer> value= new Tuple2<>(tuple._1()._1(),tuple._2());
        return new Tuple2<Tuple2<String,String>, Tuple2<Long,Integer>>(key, value);
    };

                                                                             //Time, Count
    private static PairFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long, Integer>>>, Tuple2<String, String>, Iterable<Tuple2<Long,Integer>>> makeTimeContinuous = (tuple) -> {
        List<Tuple2<Long, Integer>> timeCountPairList = Lists.newArrayList(tuple._2());
        long startTime = 437086L;
        long timeSpan = 24*7;
        long endTime = startTime + timeSpan;
        for (long i = startTime; i <= endTime; i++){//check if the time tick exist in the list, add if not
            Boolean timeExist = false;
            for (Tuple2 token:timeCountPairList){
                if((long)token._1() == i)
                    timeExist = true;
            }
            if (!timeExist)
                timeCountPairList.add(new Tuple2<>(i,0));
        }



        Collections.sort(timeCountPairList, new Comparator<Tuple2<Long, Integer>>() {
            @Override
            public int compare(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) {
                return (int)(t1._1()-t2._1());
            }
        });
        return new Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long,Integer>>>(tuple._1(),timeCountPairList);
    };

    private static PairFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long, Integer>>>, Tuple2<String, String>, Iterable<Tuple2<Long,Double>>> takeGradient = (tuple) -> {
        List<Tuple2<Long, Integer>> timeCountPairList = Lists.newArrayList(tuple._2());
        List<Tuple2<Long, Double>> timeGradientPairList = new ArrayList<>();
        long startTime = 437086L;
        int interval = 6;
        int bias = 1;
        for (int i = 0 + interval; i <= timeCountPairList.size() - interval; i++){
            double sumLatter = 0;
            double sumPrior = 0;
            double avgLatter = 0;
            double avgPrior = 0;
            double gradient = 0;
            for (int j = 0; j < interval; j++)
                sumLatter += timeCountPairList.get(i+j)._2();
            for (int j = 1; j <= interval; j++)
                sumPrior += timeCountPairList.get(i-j)._2();
            avgLatter = sumLatter / interval;
            avgPrior = sumPrior / interval;
            gradient = (avgLatter - avgPrior)/(avgPrior + bias);
            timeGradientPairList.add(new Tuple2<>(startTime + i, gradient));
        }
        return new Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long,Double>>>(tuple._1(),timeGradientPairList);
    };
    private static PairFlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long, Double>>>, Tuple2<String, String>, Tuple2<Long,Double>> formatToRows = (tuple) -> {
        List<Tuple2<Long, Double>> timeCountPairList = Lists.newArrayList(tuple._2());
        List<Tuple2<Tuple2<String, String>, Tuple2<Long,Double>>> outputList = new ArrayList<>();
        for (Tuple2 row:tuple._2())
             outputList.add(new Tuple2<Tuple2<String, String>, Tuple2<Long,Double>>(tuple._1(), row));
        return outputList.iterator();
    };
    private static PairFlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<Tuple2<Long, Integer>>>, Tuple2<String, String>, Tuple2<Long,Integer>> formatToRows1 = (tuple) -> {
        List<Tuple2<Tuple2<String, String>, Tuple2<Long,Integer>>> outputList = new ArrayList<>();
        for (Tuple2 row:tuple._2())
            outputList.add(new Tuple2<Tuple2<String, String>, Tuple2<Long,Integer>>(tuple._1(), row));
        return outputList.iterator();
    };
    private static Function<Tuple2<Tuple2<String, String>, Tuple2<Long,Double>>, String> formatToLine = (tuple) -> {
        String newLine = tuple._1()._1() + "," + tuple._1()._2() + "," + tuple._2()._1() +","+ tuple._2()._2();
        return newLine;
    };
    private static Function<Tuple2<Tuple2<String, String>, Tuple2<Long,Integer>>, String> formatToLine1 = (tuple) -> {
        String newLine = tuple._1()._1() + "," + tuple._1()._2() + "," + tuple._2()._1() +","+ tuple._2()._2();
        return newLine;
    };


    private static Map<String, String> STATE_MAP;
    static {
        STATE_MAP = new HashMap<String, String>();
        STATE_MAP.put("AL", "Alabama");
        STATE_MAP.put("AK", "Alaska");
        STATE_MAP.put("AB", "Alberta");
        STATE_MAP.put("AZ", "Arizona");
        STATE_MAP.put("AR", "Arkansas");
        STATE_MAP.put("BC", "British Columbia");
        STATE_MAP.put("CA", "California");
        STATE_MAP.put("CO", "Colorado");
        STATE_MAP.put("CT", "Connecticut");
        STATE_MAP.put("DE", "Delaware");
        STATE_MAP.put("DC", "District Of Columbia");
        STATE_MAP.put("FL", "Florida");
        STATE_MAP.put("GA", "Georgia");
        STATE_MAP.put("GU", "Guam");
        STATE_MAP.put("HI", "Hawaii");
        STATE_MAP.put("ID", "Idaho");
        STATE_MAP.put("IL", "Illinois");
        STATE_MAP.put("IN", "Indiana");
        STATE_MAP.put("IA", "Iowa");
        STATE_MAP.put("KS", "Kansas");
        STATE_MAP.put("KY", "Kentucky");
        STATE_MAP.put("LA", "Louisiana");
        STATE_MAP.put("ME", "Maine");
        STATE_MAP.put("MB", "Manitoba");
        STATE_MAP.put("MD", "Maryland");
        STATE_MAP.put("MA", "Massachusetts");
        STATE_MAP.put("MI", "Michigan");
        STATE_MAP.put("MN", "Minnesota");
        STATE_MAP.put("MS", "Mississippi");
        STATE_MAP.put("MO", "Missouri");
        STATE_MAP.put("MT", "Montana");
        STATE_MAP.put("NE", "Nebraska");
        STATE_MAP.put("NV", "Nevada");
        STATE_MAP.put("NB", "New Brunswick");
        STATE_MAP.put("NH", "New Hampshire");
        STATE_MAP.put("NJ", "New Jersey");
        STATE_MAP.put("NM", "New Mexico");
        STATE_MAP.put("NY", "New York");
        STATE_MAP.put("NF", "Newfoundland");
        STATE_MAP.put("NC", "North Carolina");
        STATE_MAP.put("ND", "North Dakota");
        STATE_MAP.put("NT", "Northwest Territories");
        STATE_MAP.put("NS", "Nova Scotia");
        STATE_MAP.put("NU", "Nunavut");
        STATE_MAP.put("OH", "Ohio");
        STATE_MAP.put("OK", "Oklahoma");
        STATE_MAP.put("ON", "Ontario");
        STATE_MAP.put("OR", "Oregon");
        STATE_MAP.put("PA", "Pennsylvania");
        STATE_MAP.put("PE", "Prince Edward Island");
        STATE_MAP.put("PR", "Puerto Rico");
        STATE_MAP.put("QC", "Quebec");
        STATE_MAP.put("RI", "Rhode Island");
        STATE_MAP.put("SK", "Saskatchewan");
        STATE_MAP.put("SC", "South Carolina");
        STATE_MAP.put("SD", "South Dakota");
        STATE_MAP.put("TN", "Tennessee");
        STATE_MAP.put("TX", "Texas");
        STATE_MAP.put("UT", "Utah");
        STATE_MAP.put("VT", "Vermont");
        STATE_MAP.put("VI", "Virgin Islands");
        STATE_MAP.put("VA", "Virginia");
        STATE_MAP.put("WA", "Washington");
        STATE_MAP.put("WV", "West Virginia");
        STATE_MAP.put("WI", "Wisconsin");
        STATE_MAP.put("WY", "Wyoming");
        STATE_MAP.put("YT", "Yukon Territory");
    }

}


/*
        JavaRDD<String>column0 = textFileRDD.map(f->f.split("\t")[0]);
        JavaRDD<String>column1 = textFileRDD.map(f->f.split("\t")[1]);
        JavaRDD<String>column2 = textFileRDD.map(f->f.split("\t")[2]);
        JavaRDD<String>column3a = textFileRDD.map(f->f.split("\t")[3]);
        JavaRDD<String>column3b = textFileRDD.map(f->f.split("\t")[f.split("\t").length-2]);
        JavaRDD<String>column5 = textFileRDD.map(f->f.split("\t")[f.split("\t").length-1]);
        //column0.saveAsTextFile(args[1]+"/column0");
        //column1.saveAsTextFile(args[1]+"/column1");
        //column2.saveAsTextFile(args[1]+"/column2");
        //column3.saveAsTextFile(args[1]+"/column3");
        column3a.saveAsTextFile(args[1]+"/column4a");
        column3b.saveAsTextFile(args[1]+"/column4b");
        //column5.saveAsTextFile(args[1]+"/column5");
*/

/*
    JavaPairRDD<Tuple2<String, String>, Tuple2<Optional<Tuple2<Integer,Integer>>,Optional<Tuple2<Integer,Integer>>>> joined =
            useWordStateAsKey.fullOuterJoin(useWordStateAsKey).distinct();
JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>> joinedRemovedOptional =
        joined.mapToPair(removeOptional);
 //Filter by time within one hour
        JavaPairRDD<Tuple2<String, String>, Tuple2<Tuple2<Integer,Integer>,Tuple2<Integer,Integer>>> filteredByTimeInterval =joinedRemovedOptional.filter(s-> (s._2._1._1 - s._2._2._1 == 1));
*/