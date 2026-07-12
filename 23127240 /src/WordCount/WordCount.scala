import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
 * Mapper:
 * - Reads one dictionary entry per line.
 * - Converts the first non-whitespace character to lowercase.
 * - Emits (letter, 1) only for f, i, t, h, c, m, u, s.
 *
 * Leading punctuation is NOT removed.
 * Example:
 *   Future  -> (f, 1)
 *   f-test  -> (f, 1)
 *   !future -> ignored because the first character is '!'
 */
class StartsWithMapper
    extends Mapper[Object, Text, Text, IntWritable] {

  private val one = new IntWritable(1)
  private val outKey = new Text()

  private val targetLetters =
    Set('f', 'i', 't', 'h', 'c', 'm', 'u', 's')

  override def map(
      key: Object,
      value: Text,
      context: Mapper[Object, Text, Text, IntWritable]#Context
  ): Unit = {

    // Remove surrounding whitespace only.
    // Punctuation inside the word is preserved.
    val word = value.toString.trim

    if (word.nonEmpty) {
      val firstChar = word.charAt(0).toLower

      if (targetLetters.contains(firstChar)) {
        outKey.set(firstChar.toString)
        context.write(outKey, one)
      }
    }
  }
}

/**
 * Reducer:
 * Adds all counts belonging to the same starting letter.
 */
class SumReducer
    extends Reducer[Text, IntWritable, Text, IntWritable] {

  override def reduce(
      key: Text,
      values: java.lang.Iterable[IntWritable],
      context: Reducer[Text, IntWritable, Text, IntWritable]#Context
  ): Unit = {

    var sum = 0
    val iterator = values.iterator()

    while (iterator.hasNext) {
      sum += iterator.next().get()
    }

    context.write(key, new IntWritable(sum))
  }
}

/**
 * Configures and runs the Hadoop MapReduce job.
 */
class WordCountDriver extends Configured with Tool {

  override def run(args: Array[String]): Int = {

    if (args.length != 2) {
      System.err.println(
        "Usage: WordCount <input_path> <output_path>"
      )
      return 2
    }

    val job = Job.getInstance(
      getConf,
      "Count words starting with f i t h c m u s"
    )

    job.setJarByClass(classOf[WordCountDriver])

    job.setMapperClass(classOf[StartsWithMapper])

    // Local partial aggregation before data is sent to Reducer.
    job.setCombinerClass(classOf[SumReducer])

    job.setReducerClass(classOf[SumReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // One Reducer -> one part-r-00000 output file.
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(
      job,
      new Path(args(0))
    )

    FileOutputFormat.setOutputPath(
      job,
      new Path(args(1))
    )

    if (job.waitForCompletion(true)) 0 else 1
  }
}

/**
 * Entry point of the application.
 */
object WordCount {

  def main(args: Array[String]): Unit = {

    val exitCode = ToolRunner.run(
      new Configuration(),
      new WordCountDriver(),
      args
    )

    System.exit(exitCode)
  }
}
