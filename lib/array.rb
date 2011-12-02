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

end
