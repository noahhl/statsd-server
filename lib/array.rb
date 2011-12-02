class Array 
  
  def array_sum
   inject( nil ) { |sum,x| sum ? sum+x : x }; 
  end

  def mean
   self.array_sum / self.length
  end

  def median
   self.sort[self.length/2]
  end

end
