require 'csv'
require 'rubygems'
require 'bundler/setup'
require 'neo4j'
require 'parallel'

class Challenge

  include Neo4j::ActiveNode

  @@errors = ActiveModel::Errors.new(self)
  @@file = File.expand_path('../users.csv', __FILE__)
  @@session = Neo4j::Session.open(:server_db, 'http://localhost:7474')

  def self.valid? (value)
    value.match(/^\d*$/) ? true : (@@errors.add('Not a number', 'The value #{value} is not a number.') ; false)
  end

  def self.create_node (type, value)
    valid?(value) ? Neo4j::Node.create(user: value) : nil
  end

  Parallel.each(CSV.read(@@file), :in_threads => 10) do |user_row|
    user_a_id, user_b_id = user_row
    Neo4j::Transaction.run do 
      a = create_node('a', user_a_id.delete(' '))
      b = create_node('b', user_b_id.delete(' '))
      unless (a.blank? || b.blank?)
        a.create_rel(:relationship, b)
        b.create_rel(:relationship, a)
      end
    end
  end
end