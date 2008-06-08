require 'eventmachine'
require 'thin'

# These are the core methods you will use to work with the Pipeliner 
# library.  They are added dynamically to the Kernel object so that they 
# will be available in all contexts.
module EventMachine 
  
  # Schedules the passed block to execute after an optional delay.  A 
  # Deferrable object is returned from this method that will trigger 
  # success after the block executes or fail if the deferrable expires.
  # 
  # ==== Options
  # :timeout => Maximum seconds to wait for this operation to complete.
  # :delay => Seconds to wait before executing the passed block.
  # :wait => if true, then once the deferrable is scheduled, we wait for it
  #
  def EventMachine::pipeline(opts = {}, &block)
    
    # Collect timeout
    timeout = opts[:timeout] || 0
    delay = opts[:delay] || 0
    should_wait = opts[:wait].nil? ? true : !!opts[:wait]
    
    # Create the deferrable to return
    deferrable = EM::DefaultDeferrable.new 
    deferrable.timeout(timeout) if timeout > 0

    # Now schedule a handler.
    # This will execute the block and set the success or failure state
    # on the deferrable.
    add_timer(delay) do
      begin
        ret = nil
        cont = start_pipeline { ret = block.call }
        
      # If an exception was raised, fail the deferrable.
      rescue Exception => e
        deferrable.fail(e)
        
      # If no exception, then deferrable succeeded, unless there was a 
      # continuation from the pipeline, in which case we need to just 
      # hang out for awhile longer.
      else
        deferrable.succeed(ret) if cont.nil?
      end
    end

    if should_wait
      return wait_for(deferrable) 
    else
      return deferrable
    end
  end
  
  # Resumes a paused pipeline.  When the pipeline reaches its start again,
  # it will return to this position and start to execute again.
  def EventMachine::resume_pipeline(cont, arg)
    old_resume = Thread.current[:pipeline_resume_cc]
    ret = callcc do |resume| 
      Thread.current[:pipeline_resume_cc] = resume
      cont.call(arg)
    end
    Thread.current[:pipeline_resume_cc] = old_resume
    return ret 
  end
  
  # Pauses the current execution state, returning to the pipeline root.
  # Execution will begin again when the passed deferrable object goes into
  # success or failure.
  def EventMachine::wait_for(deferrable)
    
    # get the current pipeline continuation.  Raise exception if none found.
    pipeline_cc = Thread.current[:pipeline_cc] 
    if pipeline_cc.nil?
      raise("Cannot wait for deferrable outside of pipeline") 
    end
    
    # Now setup continuation.  When this function returns, the deferrable
    # will have completed.
    status, result = callcc do |cont|
      
      # Set success and failure callbacks on deferrable
      deferrable.callback { |*args| resume_pipeline(cont, [:success, args]) }
      deferrable.errback  { |*args| resume_pipeline(cont, [:failure, args]) }
      
      # Now jump up the pipeline.  Execution can resume later.
      pipeline_cc.call(cont)
    end
    
    # Based on the status, either raise an exception or return the value.
    if status == :failure
      raise(result) 
    else
      return result  
    end
  end
  
  # This must be called someone at the root of an event handler to start
  # the pipeline.  This saves a continuation state so that any pipelined
  # events during execution of the passed block can come back to this 
  # point.
  def EventMachine::start_pipeline(&block)
    old_cc = Thread.current[:pipeline_cc] 
    ret = callcc do |cont|
      Thread.current[:pipeline_cc] = cont # save for future use
      block.call # yield to block.
      nil # if we didn't jump to cc, then return nil
    end
    
    # Clear the local continuation
    Thread.current[:pipeline_cc] = old_cc
    
    # If there is a resume cont, go to there, otherwise just return...
    pipeline_resume_cc = Thread.current[:pipeline_resume_cc]
    pipeline_resume_cc.call(ret) unless pipeline_resume_cc.nil?
    
    return ret 
  end
    
end

# Add Pipelining support to Thin.
module Thin
  class Connection

    def pipelined?
      !threaded?
    end
    
    # Override the process method to handle pipelining...
    def process
      if threaded?
        @request.threaded = true
        EventMachine.defer(method(:pre_process), method(:post_process))
      elsif pipelined?
        @request.threaded = false
        EM::start_pipeline { post_process(pre_process) }
      else
        @request.threaded = false
        post_process(pre_process)
      end
    end

    def handle_error
      log "!! Unexpected error while processing request: #{$!.message} - #{$!.backtrace.join("\n")}"
      log_error
      close_connection rescue nil
    end
        
  end
end

