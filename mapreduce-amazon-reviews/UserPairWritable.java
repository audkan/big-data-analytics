package hw2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserPairWritable implements WritableComparable<UserPairWritable> {
	String user1;
	String user2;
	
	/**
	 * Empty constructor - required for serialization
	 */
	public UserPairWritable() {
		
	}
	
	/**
	 * Constructors with two String objects provided as input.
	 */
	public UserPairWritable(String user1, String user2) {
		this.user1 = user1;
		this.user2 = user2;
	}

	/**
	 * Serializes the fields of this object to out. 
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(user1);
		out.writeUTF(user2);
	}
	
	/**
	 * Deserializes the fields of this object from in.
	 */
	public void readFields(DataInput in) throws IOException {
		user1 = in.readUTF();
		user2 = in.readUTF();
	}
	
	/**
	 * Compares this object to another UserPairWritable object by
	 * comparing the user1 first. If the user1 strings are equal,
	 * then the user2 strings are compared.
	 */
	public int compareTo(UserPairWritable other) {
		int ret = user1.compareTo(other.user1);
		if (ret == 0) {
			return user2.compareTo(other.user2);
		}
		return ret;
	}
	
	/**
	 * A custom method that returns the two user strings in the 
	 * UserPairWritable object separated by tab
	 */
	public String toString() {
		return user1 + "\t" + user2;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((user1 == null) ? 0 : user1.hashCode());
		result = prime * result + ((user2 == null) ? 0 : user2.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof UserPairWritable)) {
			return false;
		}
		UserPairWritable other = (UserPairWritable) obj;
		if (user1 == null) {
			if (other.user1 != null) {
				return false;
			}
		} else if (!user1.equals(other.user1)) {
			return false;
		}
		if (user2 == null) {
			if (other.user2 != null) {
				return false;
			}
		} else if (!user2.equals(other.user2)) {
			return false;
		}
		return true;
	}
	
	

}
