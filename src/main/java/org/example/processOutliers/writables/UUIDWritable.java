package org.example.processOutliers.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;

public class UUIDWritable implements WritableComparable<UUIDWritable> {

	private UUID uuid;

	public UUIDWritable() {
	}

	public UUIDWritable(UUID uuid) {
		this.uuid = uuid;
	}

	public void set(UUID uuid) {
		this.uuid = uuid;
	}

	public UUID get() {
		return uuid;
	}

	@Override
	public int hashCode() {
		return uuid.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof UUIDWritable)) {
			return false;
		}

		UUIDWritable other = (UUIDWritable) obj;

		return this.uuid.equals(other.uuid);
	}

	@Override
	public int compareTo(UUIDWritable o) {
		return this.uuid.compareTo(o.uuid);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(uuid.getMostSignificantBits());
		out.writeLong(uuid.getLeastSignificantBits());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.uuid = new UUID(in.readLong(), in.readLong());
	}

	@Override
	public String toString() {
		return uuid.toString();
	}
}
