package com.barenode.barecouch;

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
	private long update_seq;
	
	
	public long getCommitted_update_seq() {
		return committed_update_seq;
	}
	
	public void setCommitted_update_seq(long committed_update_seq) {
		this.committed_update_seq = committed_update_seq;
	}
	
	public boolean isCompact_running() {
		return compact_running;
	}
	
	public void setCompact_running(boolean compact_running) {
		this.compact_running = compact_running;
	}
	
	public long getData_size() {
		return data_size;
	}
	
	public void setData_size(long data_size) {
		this.data_size = data_size;
	}
	
	public String getDb_name() {
		return db_name;
	}
	
	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}
	
	public long getDisk_format_version() {
		return disk_format_version;
	}
	
	public void setDisk_format_version(long disk_format_version) {
		this.disk_format_version = disk_format_version;
	}
	
	public long getDisk_size() {
		return disk_size;
	}
	
	public void setDisk_size(long disk_size) {
		this.disk_size = disk_size;
	}
	
	public long getDoc_count() {
		return doc_count;
	}
	
	public void setDoc_count(long doc_count) {
		this.doc_count = doc_count;
	}
	
	public long getDoc_del_count() {
		return doc_del_count;
	}
	
	public void setDoc_del_count(long doc_del_count) {
		this.doc_del_count = doc_del_count;
	}
	
	public String getInstance_start_time() {
		return instance_start_time;
	}
	
	public void setInstance_start_time(String instance_start_time) {
		this.instance_start_time = instance_start_time;
	}
	
	public long getPurge_seq() {
		return purge_seq;
	}
	
	public void setPurge_seq(long purge_seq) {
		this.purge_seq = purge_seq;
	}
	
	public long getUpdate_seq() {
		return update_seq;
	}
	
	public void setUpdate_seq(long update_seq) {
		this.update_seq = update_seq;
	}
}
