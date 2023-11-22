# frozen_string_literal: true

RSpec.describe ReadySet::Status do
  describe '.fetch' do
    subject { ReadySet::Status.fetch }

    let(:rows) do
      [
        {
          'name' => 'test name',
          'value' => 'test value',
        },
      ]
    end
    let(:status) { instance_double(ReadySet::Status) }

    before do
      allow(ReadySet).to receive(:raw_query).with('SHOW READYSET STATUS').and_return(rows)
      allow(ReadySet::Status).to receive(:new).with(rows).and_return(status)

      subject
    end

    it 'invokes "SHOW READYSET STATUS" on ReadySet' do
      expect(ReadySet).to have_received(:raw_query)
    end

    it 'invokes ReadySet::Status.new with the results from "SHOW READYSET STATUS"' do
      expect(ReadySet::Status).to have_received(:new).with(rows)
    end

    it 'returns the ReadySet::Status returns by the constructor' do
      is_expected.to eq(status)
    end
  end

  describe '.new' do
    subject do
      rows = attributes_to_rows(test_status_attributes)
      ReadySet::Status.new(rows)
    end

    it "assigns the object's attributes correctly" do
      expect(subject.connection_count).to eq(5)
      expect(subject.last_completed_snapshot).to eq(Time.parse('2023-11-22 16:40:34'))
      expect(subject.last_replicator_error).to eq('test error')
      expect(subject.last_started_controller).to eq(Time.parse('2023-11-22 16:40:34'))
      expect(subject.last_started_replication).to eq(Time.parse('2023-11-22 16:40:34'))
      expect(subject.minimum_replication_offset).to eq('(0/33ED51B8, 0/33ED51E8)')
      expect(subject.maximum_replication_offset).to eq('(0/33ED51B8, 0/33ED51E8)')
      expect(subject.snapshotting_completed?).to eq(false)
      expect(subject.connected_to_database?).to eq(true)
    end
  end

  describe '#connected_to_database?' do
    subject { status.connected_to_database? }

    let(:status) do
      attrs = test_status_attributes
      attrs['Database Connection'] = database_connection

      rows = attributes_to_rows(attrs)

      ReadySet::Status.new(rows)
    end

    context 'when the ReadySet::Status#connected_to_database attribute is "Connected"' do
      let(:database_connection) { 'Connected' }

      it 'returns true' do
        is_expected.to eq(true)
      end
    end

    context 'when the ReadySet::Status#connected_to_database attribute is "Not Connected"' do
      let(:database_connection) { 'Not Connected' }

      it 'returns false' do
        is_expected.to eq(false)
      end
    end
  end

  describe '#reload' do
    subject { status.reload }

    let(:status) { ReadySet::Status.new(attributes_to_rows(test_status_attributes)) }

    before do
      attrs = {
        'Connection Count' => '10',
        'Database Connection' => 'Not Connected',
        'Last completed snapshot' => '2023-11-23 16:40:34',
        'Last replicator error' => 'test error 2',
        'Last started Controller' => '2023-11-23 16:40:34',
        'Last started replication' => '2023-11-23 16:40:34',
        'Minimum Replication Offset' => '(0/33ED51B9, 0/33ED51E9)',
        'Maximum Replication Offset' => '(0/33ED51B9, 0/33ED51E9)',
        'ReadySet Controller Status' => 'new status',
        'Snapshot Status' => 'Completed',
      }
      updated_status = ReadySet::Status.new(attributes_to_rows(attrs))

      allow(ReadySet::Status).to receive(:fetch).and_return(updated_status)

      subject
    end

    it 'updates the attributes of the query with updated data from ReadySet' do
      expect(subject.connection_count).to eq(10)
      expect(subject.connected_to_database?).to eq(false)
      expect(subject.last_completed_snapshot).to eq(Time.parse('2023-11-23 16:40:34'))
      expect(subject.last_replicator_error).to eq('test error 2')
      expect(subject.last_started_controller).to eq(Time.parse('2023-11-23 16:40:34'))
      expect(subject.last_started_replication).to eq(Time.parse('2023-11-23 16:40:34'))
      expect(subject.minimum_replication_offset).to eq('(0/33ED51B9, 0/33ED51E9)')
      expect(subject.maximum_replication_offset).to eq('(0/33ED51B9, 0/33ED51E9)')
      expect(subject.controller_status).to eq('new status')
      expect(subject.snapshotting_completed?).to eq(true)
    end
  end

  describe '#snapshotting_completed?' do
    subject { status.snapshotting_completed? }

    let(:status) do
      attrs = test_status_attributes
      attrs['Snapshot Status'] = snapshot_status

      rows = attributes_to_rows(attrs)

      ReadySet::Status.new(rows)
    end

    context 'when the ReadySet::Status#snapshot_status attribute is "Completed"' do
      let(:snapshot_status) { 'Completed' }

      it 'returns true' do
        is_expected.to eq(true)
      end
    end

    context 'when the ReadySet::Status#snapshot_status attribute is "In Progress"' do
      let(:snapshot_status) { 'In Progress' }

      it 'returns false' do
        is_expected.to eq(false)
      end
    end
  end

  private

  def test_status_attributes
    {
      'Connection Count' => '5',
      'Database Connection' => 'Connected',
      'Last completed snapshot' => '2023-11-22 16:40:34',
      'Last replicator error' => 'test error',
      'Last started Controller' => '2023-11-22 16:40:34',
      'Last started replication' => '2023-11-22 16:40:34',
      'Minimum Replication Offset' => '(0/33ED51B8, 0/33ED51E8)',
      'Maximum Replication Offset' => '(0/33ED51B8, 0/33ED51E8)',
      'ReadySet Controller Status' => nil,
      'Snapshot Status' => 'Snapshotting',
    }
  end

  def attributes_to_rows(attrs)
    attrs.each_with_object([]) do |(k, v), rows|
      rows.push({ 'name' => k, 'value' => v })
    end
  end
end
