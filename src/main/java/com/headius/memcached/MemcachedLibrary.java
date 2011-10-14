package com.headius.memcached;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.rubyeye.xmemcached.GetsResponse;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.transcoders.CachedData;
import net.rubyeye.xmemcached.transcoders.Transcoder;
import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyModule;
import org.jruby.RubyObject;
import org.jruby.RubyString;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ClassIndex;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.Library;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

public class MemcachedLibrary implements Library {
    private Ruby ruby;
    private RubyClass getsResponseClass;
    
    public void load(Ruby ruby, boolean bln) throws IOException {
        this.ruby = ruby;
        
        RubyModule jruby = ruby.getOrCreateModule("JRuby");
        RubyClass memcached = jruby.defineClassUnder("Memcached", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass rc) {
                return new _MemcachedClient(ruby, rc);
            }
        });
        
        memcached.defineAnnotatedMethods(_MemcachedClient.class);
        
        getsResponseClass = memcached.defineClassUnder("GetsResponse", ruby.getObject(), ObjectAllocator.NOT_ALLOCATABLE_ALLOCATOR);
    }
    
    public class _MemcachedClient extends RubyObject {
        private MemcachedClient client;
        private Transcoder<IRubyObject> transcoder;
        
        public _MemcachedClient(final Ruby ruby, RubyClass rubyClass) {
            super(ruby, rubyClass);
            
            transcoder = new Transcoder<IRubyObject>() {
                static final int STRING = 0;
                
                public CachedData encode(IRubyObject t) {
                    try {
                        switch (t.getMetaClass().index) {
                            case ClassIndex.STRING:
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                MarshalStream marshal = new MarshalStream(ruby, baos, Integer.MAX_VALUE);
                                marshal.dumpObject(t);
                                return new CachedData(STRING, baos.toByteArray());
                            default:
                                throw new UnsupportedOperationException("Not supported yet.");
                        }
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

                public void setPrimitiveAsString(boolean bln) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                public void setPackZeros(boolean bln) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                public void setCompressionThreshold(int i) {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                public boolean isPrimitiveAsString() {
                    throw new UnsupportedOperationException("Not supported yet.");
                }

                public boolean isPackZeros() {
                    throw new UnsupportedOperationException("Not supported yet.");
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
                client = new XMemcachedClient(serverSocks);
            } catch (IOException ioe) {
                throw context.runtime.newIOErrorFromException(ioe);
            }
            
            return context.nil;
        }
        
        @JRubyMethod
        public IRubyObject set(ThreadContext context, IRubyObject key, IRubyObject value) {
            try {
                return ruby.newBoolean(client.set(key.toString(), 0, value, transcoder));
            } catch (TimeoutException te) {
                throw context.runtime.newRuntimeError(te.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject set_lazy(ThreadContext context, IRubyObject key, IRubyObject value) {
            try {
                client.setWithNoReply(key.toString(), 0, value, transcoder);
                return context.nil;
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject get(ThreadContext context, IRubyObject key) {
            try {
                return client.get(key.toString(), transcoder);
            } catch (TimeoutException te) {
                throw context.runtime.newRuntimeError(te.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject get_multi(ThreadContext context, IRubyObject keys) {
            try {
                Map<String, IRubyObject> results = client.get((List<String>)keys.convertToArray(), transcoder);
                RubyHash _results = RubyHash.newHash(ruby);
                for (Map.Entry<String, IRubyObject> entry : results.entrySet()) {
                    _results.op_aset(context, ruby.newString(entry.getKey()), entry.getValue());
                }
                return _results;
            } catch (TimeoutException te) {
                throw context.runtime.newRuntimeError(te.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject get_lazy(ThreadContext context, IRubyObject key) {
            try {
                return new _GetsResponse(client.<IRubyObject>gets(key.toString(), transcoder));
            } catch (TimeoutException te) {
                throw context.runtime.newRuntimeError(te.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
        
        @JRubyMethod
        public IRubyObject get_lazy_multi(ThreadContext context, IRubyObject keys) {
            try {
                Map<String, GetsResponse<IRubyObject>> results = client.gets((List<String>)keys.convertToArray(), transcoder);
                RubyHash _results = RubyHash.newHash(ruby);
                for (Map.Entry<String, GetsResponse<IRubyObject>> entry : results.entrySet()) {
                    _results.op_aset(context, ruby.newString(entry.getKey()), new _GetsResponse(entry.getValue()));
                }
                return _results;
            } catch (TimeoutException te) {
                throw context.runtime.newRuntimeError(te.getLocalizedMessage());
            } catch (InterruptedException ie) {
                throw context.runtime.newThreadError(ie.getLocalizedMessage());
            } catch (MemcachedException me) {
                throw context.runtime.newRuntimeError(me.getLocalizedMessage());
            }
        }
    }
    
    public class _GetsResponse extends RubyObject {
        private GetsResponse<IRubyObject> response;
        
        public _GetsResponse(GetsResponse<IRubyObject> response) {
            super(ruby, getsResponseClass);
            this.response = response;
        }
        
        @JRubyMethod
        public IRubyObject value() {
            return response.getValue();
        }
    }
}
