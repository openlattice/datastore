package com.dataloom.datastore.util;

import java.util.Iterator;

public class JacksonCassandraIterableWrapper<T> implements Iterable<T> {
    private final Iterable<T> iterable;

    public JacksonCassandraIterableWrapper( Iterable<T> iterable ) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<T> iterator() {
        return new JacksonCassandraIteratorWrapper<T>( iterable.iterator() );
    }

    public static <T> Iterable<T> wrap( Iterable<T> i ) {
        return new JacksonCassandraIterableWrapper<T>( i );
    }

    public static class JacksonCassandraIteratorWrapper<T> implements Iterator<T> {
        private final Iterator<T> iterator;

        public JacksonCassandraIteratorWrapper( Iterator<T> iterator ) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public T next() {
            return iterator.next();
        }

    }

}
