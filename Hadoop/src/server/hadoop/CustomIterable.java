package hadoop;

import java.util.Iterator;

//class GenericType<GT> {
//    GT obT;
//
//    GenericType(GT o) {
//        obT = o;
//    }
//
//    GT getValue() {
//        return obT;
//    }
//
//    void showType() {
//        System.out.println("Type of GT is " + obT.getClass().getName());
//    }
//}

public class CustomIterable{

    private Iterator iterator;
    public Class<?> dataType;

    public CustomIterable(Iterator itr) {
        this.iterator = itr;
    }

    public void setDataType(Class dataType) {
        this.dataType = dataType;
    }

    public boolean hasNext() {
        return iterator.hasNext();
    }

    public IntWritable next() throws Exception{
        if (dataType.getClass().equals(IntWritable.class)) {
            IntWritable value = new IntWritable();
            value.set(Integer.parseInt((String) iterator.next()));
        } else if (dataType.getClass().equals(Text.class)) {
            Text value = new Text();
            value.set((String) iterator.next());
        }
        return null;

//        GenericType<T> returnValue = new GenericType<T>(Integer.parseInt((String)iterator.next()));
//        GenericType<T> returnValue = new GenericType<T>(iterator.next());
    }
}


//
//    public CustomIterable(int start, int end) {
//        this.cursor = start;
//        this.end = end;
//    }
//
//    public T next() {
//
//        if (this.hasNext()) {
//            int current = cursor;
//            cursor++;
//            GenericType<T> returnValue=new GenericType<T>(current);
//            return returnValue.getValue();
//        }
//    }
//
//    public boolean hasNext() {
//        return this.cursor < end;
//    }
//
//    public void remove() {
//        throw new UnsupportedOperationException();
//    }