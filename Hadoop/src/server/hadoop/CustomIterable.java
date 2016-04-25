package hadoop;

import java.util.Iterator;

public class CustomIterable<T> {

    private Iterator iterator;
    public Class<?> dataType;

    public CustomIterable(Iterator itr) {
        this.iterator = itr;
    }

    public void setDataType(Class dataType) {
        this.dataType = dataType;
    }

    /**
     *Returns whether or not there is a next token in the iterator
     * @return boolean - true or false whether there is a next token in iterator
     */
    public boolean hasNext() {
        return iterator.hasNext();
    }

    
    /**
     * Returns next element
     * @return - Generic data type
     * @throws Exception
     */
    public T next() throws Exception {
        if (dataType.equals(IntWritable.class)) {
            //wordcount and wordmedian
            IntWritable value = new IntWritable();
            value.set(Integer.parseInt((String) iterator.next()));
//            System.out.println("Returning intwritable - "+value.get());
            return (T)value;
        } else if (dataType.equals(Text.class)) {
            //A2
            Text value = new Text();
            value.set((String) iterator.next());
//            System.out.println("returning string - "+value.get());
            return (T)value;
        }
        return null;
    }
}



