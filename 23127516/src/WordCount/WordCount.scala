import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount {

  val targetLetters = Set('f', 'i', 't', 'h', 'c', 'm', 'u', 's')

  class TokenizerMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    val outKey = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, IntWritable]#Context
    ): Unit = {

      val words = value.toString.split("\\s+")

      for (word <- words) {
        if (word.length > 0) {
          val firstChar = word.charAt(0).toLower

          if (targetLetters.contains(firstChar)) {
            outKey.set(firstChar.toString)
            context.write(outKey, one)
          }
        }
      }
    }
  }

  class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    val result = new IntWritable()

    override def reduce(
        key: Text,
        values: Iterable[IntWritable],
        context: Reducer[Text, IntWritable, Text, IntWritable]#Context
    ): Unit = {

      var sum = 0
      val iterator = values.iterator()

      while (iterator.hasNext) {
        sum += iterator.next().get()
      }

      result.set(sum)
      context.write(key, result)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input> <output>")
      System.exit(1)
    }

    val conf = new Configuration()
    val job = Job.getInstance(conf, "WordCount Warm Up")

    job.setJarByClass(WordCount.getClass)

    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
