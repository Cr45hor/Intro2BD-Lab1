import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount {

  val targetLetters =
    Array('f', 'i', 't', 'h', 'c', 'm', 'u', 's')

  class TokenizerMapper
      extends Mapper[
        LongWritable,
        Text,
        IntWritable,
        IntWritable
      ] {

    val outKey = new IntWritable()
    val one = new IntWritable(1)
    val zero = new IntWritable(0)

    override def setup(
        context: Mapper[
          LongWritable,
          Text,
          IntWritable,
          IntWritable
        ]#Context
    ): Unit = {

      var index = 0

      while (index < targetLetters.length) {
        outKey.set(index)
        context.write(outKey, zero)
        index += 1
      }
    }

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[
          LongWritable,
          Text,
          IntWritable,
          IntWritable
        ]#Context
    ): Unit = {

      val tokenizer =
        new StringTokenizer(value.toString)

      while (tokenizer.hasMoreTokens) {

        val word = tokenizer.nextToken()

        if (word.nonEmpty) {

          val firstChar =
            Character.toLowerCase(word.charAt(0))

          val index =
            targetLetters.indexOf(firstChar)

          if (index >= 0) {
            outKey.set(index)
            context.write(outKey, one)
          }
        }
      }
    }
  }

  class IntSumReducer
      extends Reducer[
        IntWritable,
        IntWritable,
        Text,
        IntWritable
      ] {

    val outKey = new Text()
    val result = new IntWritable()

    override def reduce(
        key: IntWritable,
        values: java.lang.Iterable[IntWritable],
        context: Reducer[
          IntWritable,
          IntWritable,
          Text,
          IntWritable
        ]#Context
    ): Unit = {

      var sum = 0
      val iterator = values.iterator()

      while (iterator.hasNext) {
        sum += iterator.next().get()
      }

      val letter =
        targetLetters(key.get())

      outKey.set(letter.toString)
      result.set(sum)

      context.write(outKey, result)
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println(
        "Usage: WordCount <input-path> <output-path>"
      )

      System.exit(1)
    }

    val conf = new Configuration()

    val job =
      Job.getInstance(conf, "WordCount Warm Up")

    job.setJarByClass(WordCount.getClass)

    job.setMapperClass(
      classOf[TokenizerMapper]
    )

    job.setReducerClass(
      classOf[IntSumReducer]
    )

    job.setMapOutputKeyClass(
      classOf[IntWritable]
    )

    job.setMapOutputValueClass(
      classOf[IntWritable]
    )

    job.setOutputKeyClass(
      classOf[Text]
    )

    job.setOutputValueClass(
      classOf[IntWritable]
    )

    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(
      job,
      new Path(args(0))
    )

    FileOutputFormat.setOutputPath(
      job,
      new Path(args(1))
    )

    System.exit(
      if (job.waitForCompletion(true)) 0 else 1
    )
  }
}