package ca.barelabs.barecouch;

public class DatabaseInfo {
	
	private long committed_update_seq;
	private boolean compact_running;
	private long data_size;
	private String db_name;
	private long disk_format_version;
	private long disk_size;
	private long doc_count;
	private long doc_del_count;
	private String instance_start_time;
	private long purge_seq;
	private String update_seq;
	
	
	public long getCommittedUpdateSeq() {
		return committed_update_seq;
	}
	
	public boolean isCompactRunning() {
		return compact_running;
	}
	
	public long getDataSize() {
		return data_size;
	}
	
	public String getDbName() {
		return db_name;
	}
	
	public long getDiskFormatVersion() {
		return disk_format_version;
	}
	
	public long getDiskSize() {
		return disk_size;
	}
	
	public long getDocCount() {
		return doc_count;
	}
	
	public long getDocDelCount() {
		return doc_del_count;
	}
	
	public String getInstanceStartTime() {
		return instance_start_time;
	}
	
	public long getPurgeSeq() {
		return purge_seq;
	}
	
	public String getUpdateSeq() {
		return update_seq;
	}
}
