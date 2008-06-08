class Foo < Merb::Controller

  def _template_location(action, type = nil, controller = controller_name)
    controller == "layout" ? "layout.#{action}.#{type}" : "#{action}.#{type}"
  end

  def pipelined
    t = Time.now.to_i
    ret = EM::pipeline(:delay => 0.1) do
      "OK elapsed: #{Time.now.to_i - t}"
    end
    return ret
  end

  def regular
    t = Time.now.to_i
    sleep 0.1
    return "OK elapsed: #{Time.now.to_i - t}"
  end
  
  def foo
    render
  end
  
end