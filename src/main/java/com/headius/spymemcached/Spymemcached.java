package com.headius.spymemcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

public class Spymemcached implements Library {
    private Ruby ruby;
    private RubyClass operationFutureClass;
    private RubyClass getFutureClass;
    private RubyClass bulkFutureClass;
    
    public void load(Ruby ruby, boolean bln) throws IOException {
        this.ruby = ruby;
        
        RubyModule spymemcached = ruby.defineModule("Spymemcached");
        RubyClass memcachedClient = spymemcached.defineClassUnder("MemcachedClient", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rc) {
                return new _MemcachedClient(ruby, rc);
            }
        });
        
        memcachedClient.defineAnnotatedMethods(_MemcachedClient.class);
        
        operationFutureClass = spymemcached.defineClassUnder("OperationFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
        getFutureClass = spymemcached.defineClassUnder("GetFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
        bulkFutureClass = spymemcached.defineClassUnder("BulkFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    }
    
    public class _MemcachedClient extends RubyObject {
        private MemcachedClient client;
        private Transcoder<IRubyObject> transcoder;
        
        public _MemcachedClient(final Ruby ruby, RubyClass rubyClass) {
            super(ruby, rubyClass);
            
            transcoder = new Transcoder<IRubyObject>() {
                static final int STRING = 0;

                public boolean asyncDecode(CachedData cd) {
                    return false;
                }
                
                public CachedData encode(IRubyObject t) {
                    try {
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        MarshalStream marshal = new MarshalStream(ruby, baos, Integer.MAX_VALUE);
                        marshal.dumpObject(t);
                        byte[] bytes = baos.toByteArray();
                        return new CachedData(STRING, bytes, bytes.length);
                    } catch (IOException ioe) {
                        throw ruby.newIOErrorFromException(ioe);
                    }
                }

                public IRubyObject decode(CachedData cd) {
                    try {
                        return new UnmarshalStream(ruby, new ByteArrayInputStream(cd.getData()), null, false, false).unmarshalObject();
                    } catch (IOException ioe) {
                        throw ruby.newIOErrorFromException(ioe);
                    }
                }

                public int getMaxSize() {
                    return CachedData.MAX_SIZE;
                }
            };
        }
        
        @JRubyMethod
        public IRubyObject initialize(ThreadContext context, IRubyObject servers) {
            ArrayList<InetSocketAddress> serverSocks = new ArrayList();
            
            for (String server : (List<String>)servers.convertToArray()) {
                String[] hostPort = server.split(":");
                serverSocks.add(new InetSocketAddress(hostPort[0], Integer.parseInt(hostPort[1])));
            }
            
            try {
                client = new MemcachedClient(serverSocks);
            } catch (IOException ioe) {
                throw context.runtime.newIOErrorFromException(ioe);
            }
            
            return context.nil;
        }
        
        @JRubyMethod
        public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value) {
            try {
                return ruby.newBoolean(client.set(key.toString(), 0, value, transcoder).get());
            } catch (ExecutionException ee) {
                throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject async_set(ThreadContext context, IRubyObject key, IRubyObject value) {
            return new _OperationFuture(client.set(key.toString(), 0, value, transcoder));
        }
        
        @JRubyMethod
        public IRubyObject get(ThreadContext context, IRubyObject key) {
            try {
                return client.asyncGet(key.toString(), transcoder).get();
            } catch (ExecutionException ee) {
                throw ruby.newThreadError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw ruby.newThreadError(ie.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject multiget(ThreadContext context, IRubyObject keys) {
            Map<String, IRubyObject> results = client.getBulk((List<String>)keys.convertToArray(), transcoder);
            RubyHash _results = RubyHash.newHash(ruby);
            for (Map.Entry<String, IRubyObject> entry : results.entrySet()) {
                _results.op_aset(context, ruby.newString(entry.getKey()), entry.getValue());
            }
            return _results;
        }
        
        @JRubyMethod
        public IRubyObject async_get(ThreadContext context, IRubyObject key) {
            return new _GetFuture(client.asyncGet(key.toString(), transcoder));
        }
        
        @JRubyMethod
        public IRubyObject async_multiget(ThreadContext context, IRubyObject keys) {
            return new _BulkFuture(client.asyncGetBulk((List<String>)keys.convertToArray(), transcoder));
        }
        
        @JRubyMethod
        public IRubyObject shutdown(ThreadContext context) {
            client.shutdown();
            
            return context.nil;
        }
    }
    
    public class _OperationFuture extends RubyObject {
        private OperationFuture<Boolean> future;
        
        public _OperationFuture(OperationFuture<Boolean> response) {
            super(ruby, operationFutureClass);
            this.future = response;
        }
        
        @JRubyMethod
        public IRubyObject get() {
            try {
                return ruby.newBoolean(future.get());
            } catch (ExecutionException ee) {
                throw ruby.newThreadError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw ruby.newThreadError(ie.getLocalizedMessage());
            }
        }
    }
    
    public class _GetFuture extends RubyObject {
        private GetFuture<IRubyObject> future;
        
        public _GetFuture(GetFuture<IRubyObject> future) {
            super(ruby, getFutureClass);
            this.future = future;
        }
        
        @JRubyMethod
        public IRubyObject get() {
            try {
                return future.get();
            } catch (ExecutionException ee) {
                throw ruby.newThreadError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw ruby.newThreadError(ie.getLocalizedMessage());
            }
        }
    }
    
    public class _BulkFuture extends RubyObject {
        private BulkFuture<Map<String, IRubyObject>> bulkFuture;
        
        public _BulkFuture(BulkFuture<Map<String, IRubyObject>> bulkFuture) {
            super(ruby, bulkFutureClass);
            this.bulkFuture = bulkFuture;
        }

        @JRubyMethod
        public IRubyObject get(ThreadContext context) {
            try {
                RubyHash _results = RubyHash.newHash(ruby);
                for (Map.Entry<String, IRubyObject> entry : bulkFuture.get().entrySet()) {
                    _results.op_aset(context, ruby.newString(entry.getKey()), entry.getValue());
                }
                return _results;
            } catch (ExecutionException ee) {
                throw ruby.newThreadError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw ruby.newThreadError(ie.getLocalizedMessage());
            }
        }
    }
}
