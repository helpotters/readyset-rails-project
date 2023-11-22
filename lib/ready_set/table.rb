require 'active_model'

module ReadySet
  # Represents a table from the database as it is known by ReadySet.
  class Table
    include ActiveModel::AttributeMethods

    # An error raised when a table is expected to be replicated but isn't.
    class NotReplicatedError < StandardError
      attr_reader :description, :name

      def initialize(name, description)
        @name = name
        @description = description
      end

      def to_s
        "Table #{name} is not replicated: #{description}"
      end
    end

    attr_reader :description, :name, :status

    # Returns a list of all the tables known by ReadySet along with their statuses. This
    # information is retrieved by invoking `SHOW READYSET TABLES` on ReadySet.
    #
    # @return [Array<ReadySet::Table>]
    def self.all
      ReadySet.raw_query('SHOW READYSET TABLES').map { |result| new(result.to_h) }
    end

    def initialize(attributes)
      @name = attributes['table']
      @status = attributes['status'].downcase.gsub(' ', '_').to_sym
      @description = attributes['description']
    end

    # Returns true if the table has finished snapshotting and false otherwise.
    #
    # @return [Boolean]
    # @raise [ReadySet::Table::NotReplicatedError] raised if the table is marked as not replicated
    def snapshotting_completed?
      if status == :not_replicated
        raise NotReplicatedError.new(name, description)
      else
        status == :snapshotted
      end
    end
  end
end
