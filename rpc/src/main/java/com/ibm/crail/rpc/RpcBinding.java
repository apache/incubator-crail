package com.ibm.crail.rpc;


public interface RpcBinding extends RpcClient {
	public void run(RpcNameNodeService service);
	
	@SuppressWarnings("unchecked")
	public static RpcBinding createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (RpcBinding.class.isAssignableFrom(nodeClass)){
			Class<? extends RpcBinding> bindingClass = (Class<? extends RpcBinding>) nodeClass;
			RpcBinding bindingInstance = bindingClass.newInstance();
			return bindingInstance;
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
	}		
}
