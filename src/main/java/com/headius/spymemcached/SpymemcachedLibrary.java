package com.headius.spymemcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyInteger;
import org.jruby.RubyObject;
import org.jruby.RubySymbol;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

public class SpymemcachedLibrary implements Library {
    private Ruby ruby;
    
    private RubyClass operationFutureClass;
    private RubyClass getFutureClass;
    private RubyClass bulkFutureClass;

    private Transcoder<IRubyObject> transcoder;
    
    public void load(final Ruby ruby, boolean bln) throws IOException {
        this.ruby = ruby;
        
        RubyClass spymemcached = ruby.defineClass("Spymemcached", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rc) {
                return new Spymemcached(ruby, rc);
            }
        });
        
        spymemcached.defineAnnotatedMethods(Spymemcached.class);
        
        operationFutureClass = spymemcached.defineClassUnder("OperationFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
        getFutureClass = spymemcached.defineClassUnder("GetFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
        bulkFutureClass = spymemcached.defineClassUnder("BulkFuture", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
        
        operationFutureClass.defineAnnotatedMethods(_OperationFuture.class);
        getFutureClass.defineAnnotatedMethods(_GetFuture.class);
        bulkFutureClass.defineAnnotatedMethods(_BulkFuture.class);

        transcoder = new Transcoder<IRubyObject>() {
            static final int STRING = 0;
            private final Transcoder coder = new SerializingTranscoder();

            public boolean asyncDecode(CachedData cd) {
                return false;
            }

            public CachedData encode(IRubyObject t) {
                try {
                    // If we're given a number, send it as a Java string
                    // so we can incr/decr it.
                    if (t instanceof RubyInteger) {
                        return coder.encode(t.toString());
                    }
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
                    // If we don't have our STRING flag, then it must be
                    // a number we've stored as a Java string.
                    if (cd.getFlags() != STRING) {
                        return ruby.newFixnum(Long.valueOf((String)coder.decode(cd)));
                    }
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
    
    public class Spymemcached extends RubyObject {
        private MemcachedClient client;
        private NamespaceHelper namespaceHelper;
        
        public Spymemcached(final Ruby ruby, RubyClass rubyClass) {
            super(ruby, rubyClass);
        }

        @JRubyMethod
        public IRubyObject initialize(ThreadContext context, IRubyObject servers) {
            return initialize(context, servers, context.nil);
        }
        @JRubyMethod
        public IRubyObject initialize(ThreadContext context, IRubyObject servers, IRubyObject options) {
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
            
            String namespace = null;
            if (options != context.nil) {
                RubyHash optionsHash = options.convertToHash();
                RubySymbol namespaceSymbol = RubySymbol.newSymbol(context.getRuntime(), "namespace");
                if (optionsHash.containsKey(namespaceSymbol)) {
                    namespace = optionsHash.get(namespaceSymbol).toString();
                }
            }

            this.namespaceHelper = new NamespaceHelper(namespace);
            
            return context.nil;
        }

        @JRubyMethod
        public IRubyObject add(ThreadContext context, IRubyObject key, IRubyObject value) {
            return add(context, key, value, ruby.newFixnum(0));
        }

        @JRubyMethod
        public IRubyObject add(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
            try {
                return ruby.newBoolean(client.add(namespaceHelper.namespacedKey(key), (int)timeout.convertToInteger().getLongValue(), value, transcoder).get());
            } catch (ExecutionException ee) {
                throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value) {
            return set(context, key, value, ruby.newFixnum(0));
        }
        
        @JRubyMethod
        public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
            try {
                return ruby.newBoolean(client.set(namespaceHelper.namespacedKey(key), (int)timeout.convertToInteger().getLongValue(), value, transcoder).get());
            } catch (ExecutionException ee) {
                throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject async_set(ThreadContext context, IRubyObject key, IRubyObject value) {
            return new _OperationFuture(client.set(namespaceHelper.namespacedKey(key), 0, value, transcoder));
        }
        
        @JRubyMethod
        public IRubyObject async_set(ThreadContext context, IRubyObject key, IRubyObject value, IRubyObject timeout) {
            return new _OperationFuture(client.set(namespaceHelper.namespacedKey(key), (int)timeout.convertToInteger().getLongValue(), value, transcoder));
        }
        
        @JRubyMethod
        public IRubyObject get(ThreadContext context, IRubyObject key) {
            return client.get(namespaceHelper.namespacedKey(key), transcoder);
        }
        
        @JRubyMethod
        public IRubyObject multiget(ThreadContext context, IRubyObject keys) {
            RubyHash results = RubyHash.newHash(ruby);

            for (Map.Entry<String, IRubyObject> entry : client.getBulk(namespaceHelper.namespacedKeys(keys), transcoder).entrySet()) {
                results.op_aset(context, namespaceHelper.unnamespacedKey(entry.getKey()), entry.getValue());
            }
            return results;
        }

        @JRubyMethod
        public IRubyObject async_get(ThreadContext context, IRubyObject key) {
            return new _GetFuture(client.asyncGet(namespaceHelper.namespacedKey(key), transcoder));
        }

        @JRubyMethod
        public IRubyObject async_multiget(ThreadContext context, IRubyObject keys) {
            return new _BulkFuture(client.asyncGetBulk(namespaceHelper.namespacedKeys(keys), transcoder), namespaceHelper);
        }

        @JRubyMethod
        public  IRubyObject incr(ThreadContext context, IRubyObject key) {
            return incr(context, key, ruby.newFixnum(1), ruby.newFixnum(0));
        }

        @JRubyMethod
        public IRubyObject incr(ThreadContext context, IRubyObject key, IRubyObject by) {
            return incr(context, key, by, ruby.newFixnum(0));
        }

        @JRubyMethod
        public IRubyObject incr(ThreadContext context, IRubyObject key, IRubyObject by, IRubyObject timeout) {
            long result = client.incr(namespaceHelper.namespacedKey(key), (int)by.convertToInteger().getLongValue(), 1, (int)timeout.convertToInteger().getLongValue());
            return ruby.newFixnum(result);
        }

        @JRubyMethod
        public IRubyObject decr(ThreadContext context, IRubyObject key) {
            return decr(context, key, ruby.newFixnum(1), ruby.newFixnum(0));
        }

        @JRubyMethod
        public IRubyObject decr(ThreadContext context, IRubyObject key, IRubyObject by) {
            return decr(context, key, by, ruby.newFixnum(0));
        }

        @JRubyMethod
        public IRubyObject decr(ThreadContext context, IRubyObject key, IRubyObject by, IRubyObject timeout) {
            long result = client.decr(namespaceHelper.namespacedKey(key), (int)by.convertToInteger().getLongValue(), 0, (int)timeout.convertToInteger().getLongValue());
            return ruby.newFixnum(result);
        }

        @JRubyMethod
        public IRubyObject delete(ThreadContext context, IRubyObject key) {
            try {
                return ruby.newBoolean(client.delete(namespaceHelper.namespacedKey(key)).get());
            } catch (ExecutionException ee) {
                throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            }
        }

        @JRubyMethod
        public IRubyObject clear(ThreadContext context) {
            try {
                return ruby.newBoolean(client.flush().get());
            } catch (ExecutionException ee) {
                throw context.runtime.newRuntimeError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            }
        }

        @JRubyMethod
        public IRubyObject stats(ThreadContext context) {
            RubyHash results = RubyHash.newHash(ruby);
            for(Map.Entry<SocketAddress, Map<String, String>> entry : client.getStats().entrySet()) {
                RubyHash serverHash = RubyHash.newHash(ruby);
                for(Map.Entry<String, String> server : entry.getValue().entrySet()) {
                    serverHash.op_aset(context, ruby.newString(server.getKey()), ruby.newString(server.getValue()));
                }
                results.op_aset(context, ruby.newString(entry.getKey().toString()), serverHash);
            }
            return results;
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
        public IRubyObject get(ThreadContext context) {
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
        public IRubyObject get(ThreadContext context) {
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
        private NamespaceHelper namespaceHelper;

        public _BulkFuture(BulkFuture<Map<String, IRubyObject>> bulkFuture, NamespaceHelper helper) {
            super(ruby, bulkFutureClass);
            this.bulkFuture = bulkFuture;
            this.namespaceHelper = helper;
        }

        @JRubyMethod
        public IRubyObject get(ThreadContext context) {
            try {
                RubyHash results = RubyHash.newHash(ruby);
                for (Map.Entry<String, IRubyObject> entry : bulkFuture.get().entrySet()) {
                    results.op_aset(context, this.namespaceHelper.unnamespacedKey(entry.getKey()), entry.getValue());
                }
                return results;
            } catch (ExecutionException ee) {
                throw ruby.newThreadError(ee.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw ruby.newThreadError(ie.getLocalizedMessage());
            }
        }
    }
    
    public class NamespaceHelper {
        private String namespace;
        public NamespaceHelper(String namespace) {
            this.namespace = namespace == null ? namespace : namespace + ":";
        }

        public String namespacedKey(IRubyObject key) {
            return namespace != null ? namespace +  key.toString() : key.toString();
        }

        public List<String> namespacedKeys(IRubyObject keys) {
            List<String> results = new ArrayList<String>();
            for(String key : (List<String>)keys.convertToArray()) {
                results.add(namespace == null ? key : namespace  + key);
            }
            return results;
        }

        public IRubyObject unnamespacedKey(String key) {
            return namespace != null ? ruby.newString(key.substring(namespace.length())) : ruby.newString(key);
        }
    }
}
