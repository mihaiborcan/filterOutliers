/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.processOutliers.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link org.apache.hadoop.io.Writable} encapsulating an item ID and a preference value.
 */
public final class EntityPrefWritable extends VarStringWritable implements Cloneable {

	private double prefValue;

	public EntityPrefWritable() {
		// do nothing
	}

	public EntityPrefWritable(String itemID, double prefValue) {
		super(itemID);
		this.prefValue = prefValue;
	}

	public double getPrefValue() {
		return prefValue;
	}

	public void setPrefValue(double value) {
		this.prefValue = value;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeDouble(prefValue);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		prefValue = in.readDouble();
	}

	@Override
	public int hashCode() {
		return super.hashCode() ^ Double.valueOf(prefValue).hashCode();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof EntityPrefWritable)) {
			return false;
		}
		EntityPrefWritable other = (EntityPrefWritable) o;
		return get().equals(other.get()) && prefValue == other.getPrefValue();
	}

	@Override
	public String toString() {
		return get() + "\t" + prefValue;
	}

	@Override
	public EntityPrefWritable clone() {
		return new EntityPrefWritable(get(), prefValue);
	}

}
