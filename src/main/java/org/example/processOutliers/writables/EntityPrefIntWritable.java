package org.example.processOutliers.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** A {@link org.apache.hadoop.io.Writable} encapsulating an item ID and a preference value. */
public final class EntityPrefIntWritable extends VarStringWritable implements Cloneable {

	private int prefValue;

	public EntityPrefIntWritable() {
	}

	public EntityPrefIntWritable(String itemID, int prefValue) {
		super(itemID);
		this.prefValue = prefValue;
	}

	public int getPrefValue() {
		return prefValue;
	}

	public void setPrefValue(int prefValue) {
		this.prefValue = prefValue;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(prefValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		prefValue = in.readInt();
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ prefValue;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof EntityPrefIntWritable)) {
			return false;
		}
		EntityPrefIntWritable other = (EntityPrefIntWritable) o;
		return get().equals(other.get()) && prefValue == other.getPrefValue();
	}

	@Override
	public String toString() {
		return get() + "\t" + prefValue;
	}

	@Override
	public EntityPrefIntWritable clone() {
		return new EntityPrefIntWritable(get(), prefValue);
	}

}
