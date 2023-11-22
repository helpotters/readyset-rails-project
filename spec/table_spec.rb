# frozen_string_literal: true

RSpec.describe ReadySet::Table do
  describe '.all' do
    subject { ReadySet::Table.all }

    let(:tables) do
      [
        {
          'table' => 't1',
          'status' => 'snapshotting',
          'description' => 'test description',
        },
      ]
    end

    before do
      allow(ReadySet).to receive(:raw_query).with('SHOW READYSET TABLES').and_return(tables)
      subject
    end

    it 'invokes "SHOW READYSET TABLES" on ReadySet' do
      expect(ReadySet).to have_received(:raw_query).with('SHOW READYSET TABLES')
    end

    it 'returns the tables' do
      expect(subject[0].name).to eq('t1')
      expect(subject[0].status).to eq(:snapshotting)
      expect(subject[0].description).to eq('test description')
    end
  end

  describe '.new' do
    subject { ReadySet::Table.new(attrs) }

    let(:attrs) do
      {
        'table' => 't1',
        'status' => 'snapshotting',
        'description' => 'test description',
      }
    end

    it "assigns the object's attributes correctly" do
      expect(subject.name).to eq('t1')
      expect(subject.status).to eq(:snapshotting)
      expect(subject.description).to eq('test description')
    end
  end

  describe '#snapshotting_completed?' do
    subject { table.snapshotting_completed? }

    context "when the table's status is :not_replicated" do
      let(:table) do
        ReadySet::Table.new(
          'table' => 't1',
          'status' => 'Not Replicated',
          'description' => 'test description',
        )
      end

      it 'raises a ReadySet::Table::NotReplicatedError' do
        expect { subject }.to raise_error(ReadySet::Table::NotReplicatedError)
      end
    end

    context "when the table's status is :snapshotting" do
      let(:table) do
        ReadySet::Table.new(
          'table' => 't1',
          'status' => 'Snapshotting',
          'description' => nil,
        )
      end

      it 'returns false' do
        is_expected.to eq(false)
      end
    end

    context "when the table's status is :snapshotted" do
      let(:table) do
        ReadySet::Table.new(
          'table' => 't1',
          'status' => 'Snapshotted',
          'description' => nil,
        )
      end

      it 'returns true' do
        is_expected.to eq(true)
      end
    end
  end
end
