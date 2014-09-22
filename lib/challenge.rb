require 'csv'
require 'rubygems'
require 'bundler/setup'
require 'neo4j'
require 'parallel'
require 'pry'

class Challenge

  @@errors = ActiveModel::Errors.new(self)
  @@file = File.expand_path('../users.csv', __FILE__)
  @@session = Neo4j::Session.open(:server_db, 'http://localhost:7474')

  def self.valid? (value)
    value.match(/^\d*$/) ? true : (@@errors.add('Not a number', 'The value #{value} is not a number.') ; false)
  end

  def self.find_or_create_node (value)
    if valid?(value)
      node = @@session._query("MATCH (n {user: '#{value}'}) RETURN ID(n)").data.first
      node.nil? ? Neo4j::Node.create(user: value) : Neo4j::Node.load(node["row"].first.to_i)
    else
      nil
    end
  end

  Parallel.each(CSV.read(@@file), :in_processes => 1) do |user_row|
    user_a_id, user_b_id = user_row
    Neo4j::Transaction.run do 
      a = find_or_create_node(user_a_id.delete(' '))
      b = find_or_create_node(user_b_id.delete(' '))
      unless (a.blank? || b.blank?)
        a.create_rel(:relationship, b)
        b.create_rel(:relationship, a)
      end
    end
  end
end