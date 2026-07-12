import java.util.StringTokenizer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount {

  class TokenizerMapper extends Mapper[Object, Text, IntWritable, IntWritable] {
    val targetLetters = Array('f', 'i', 't', 'h', 'c', 'm', 'u', 's')
    val one = new IntWritable(1)
    val zero = new IntWritable(0)
    val wordKey = new IntWritable()

    override def setup(context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
      for (i <- targetLetters.indices) {
        wordKey.set(i)
        context.write(wordKey, zero)
      }
    }

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, IntWritable]#Context): Unit = {
      val itr = new StringTokenizer(value.toString)
      while (itr.hasMoreTokens) {
        val word = itr.nextToken()
        if (word.nonEmpty) {
          val firstChar = word.toLowerCase.charAt(0)
          val idx = targetLetters.indexOf(firstChar)
          
          if (idx >= 0) {
            wordKey.set(idx)
            context.write(wordKey, one)
          }
        }
      }
    }
  }

  class IntSumReducer extends Reducer[IntWritable, IntWritable, Text, IntWritable] {
    val targetLetters = Array('f', 'i', 't', 'h', 'c', 'm', 'u', 's')
    val result = new IntWritable()
    val letterText = new Text()

    override def reduce(key: IntWritable, values: java.lang.Iterable[IntWritable], context: Reducer[IntWritable, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = 0
      val it = values.iterator()
      while (it.hasNext) {
        sum += it.next().get()
      }
      result.set(sum)
      letterText.set(targetLetters(key.get()).toString)
      context.write(letterText, result)
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <in> <out>")
      System.exit(2)
    }
    
    val conf = new Configuration()
    val job = Job.getInstance(conf, "word count")
    
    job.setJarByClass(this.getClass)
    job.setMapperClass(classOf[TokenizerMapper])
    job.setReducerClass(classOf[IntSumReducer])
    
    job.setMapOutputKeyClass(classOf[IntWritable])
    job.setMapOutputValueClass(classOf[IntWritable])
    
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
