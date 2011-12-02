class Array 
  
  def sum 
   inject( nil ) { |sum,x| sum ? sum+x : x }; 
  end

  def mean
   self.sum.to_f / self.length
  end

  def median
   self.sort[self.length/2]
  end

  def percentile(threshold)
    if (count > 1)
      self.sort!
      # strip off the top 100-threshold
      threshold_index = (((100 - threshold).to_f / 100) * count).round
      self[0..-threshold_index].last
    else
      self.first
    end
  end

  def mean_squared 
    # Mean squared error - to get to standard deviation, take 
    # sqrt(1/(count-1) * sme)
    m = mean
    self.map{|v| (v-m)**2}.sum
  end

  def standard_dev
    (mean_squared/(count-1))**0.5
  end

  def method_missing(method, *args, &block)
     if method.to_s =~ /^percentile_(.+)$/
       percentile($1.to_i)
     else
       super 
     end
  end

end
