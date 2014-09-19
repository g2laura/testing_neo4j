require 'csv'
require 'rubygems'
require 'bundler/setup'
require 'neo4j'
require 'parallel'
require 'pry'

class Challenge

  @@errors = ActiveModel::Errors.new(self)
  @@file = File.expand_path('../users.csv', __FILE__)
  Neo4j::Session.open(:server_db, 'http://localhost:7474')

  def self.valid? (value)
    value.match(/^\d*$/) ? true : (@@errors.add("Not a number", "The value #{value} is not a number.") ; false)
  end

  def self.create_node (type, value)
    if(valid?(value))
      (type == "a") ? Neo4j::Node.create(user_a_id: value) : Neo4j::Node.create(user_b_id: value)
    else
      nil
    end
  end

  Parallel.each(CSV.read(@@file), :in_threads => 10) do |user_row|
    user_a_id, user_b_id = user_row
    Neo4j::Transaction.run do 
      a = create_node("a", user_a_id.delete(' '))
      b = create_node("b", user_b_id.delete(' '))
      unless (a.blank? || b.blank?)
        a.create_rel(:relationship, b)
        b.create_rel(:relationship, a)
      end
    end
  end
end