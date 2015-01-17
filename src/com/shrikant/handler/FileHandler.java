package com.shrikant.handler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.service.FileStore;
import com.shrikant.service.NodeID;
import com.shrikant.service.RFile;
import com.shrikant.service.RFileMetadata;
import com.shrikant.service.SystemException;
public class FileHandler implements FileStore.Iface{
	
	public static Map<String,RFile> fileStorage;
	private static List<NodeID> nodeList;
	private static Map<String,NodeID> fingerTable;
	private static TTransport transport;
	private static TProtocol protocol;
	int portNumber;
	String deleteFile;
	
	
	public FileHandler(int port){
		
		portNumber=port;
		nodeList = new ArrayList<NodeID>();
		fileStorage = new HashMap<String,RFile>();
		
		File oldFiles = new File("./output/");
		File files[] = oldFiles.listFiles();
		for(File file:files){
			if(!(file.isDirectory())){
				file.delete();
			}
		}
		
		fingerTable = new LinkedHashMap<String,NodeID>();
	}
	@Override
	public void writeFile(RFile rFile) throws SystemException, TException {
		SystemException exception=null;
		File newFile=null;
		RFile serverRFile = null;
		RFileMetadata serverMetadata = null;
		
		RFileMetadata userMetadata = rFile.getMeta();
		String absoluteFileName = userMetadata.getFilename();
		String[] fileParts = absoluteFileName.split("\\.");
		String fileName = fileParts[0];
		String contents = rFile.getContent();
		//Check if the file already exists
		if(fileStorage.containsKey(fileName)){
			serverRFile = fileStorage.get(fileName);
			serverMetadata = serverRFile.getMeta();
			if(userMetadata.getOwner().equals(serverMetadata.getOwner())){
				//Overwrite file
				File serverFile = new File("./output/"+absoluteFileName);
				try {
					PrintWriter writer = new PrintWriter(serverFile);
					writer.write(contents);
					writer.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
				//update data structure(Time Stamps , content lenght , content hash)
				  
				  serverRFile.setContent(contents);
				  serverMetadata.setVersion(serverMetadata.getVersion()+1);
				  serverMetadata.setUpdated(System.currentTimeMillis());
				  serverMetadata.setDeleted(0);
				  serverMetadata.setContentLength(contents.length());
				  String shaKey = sha256(contents);
				  serverMetadata.setContentHash(shaKey);
				  serverRFile.setMeta(serverMetadata);
				  fileStorage.put(fileName, serverRFile);
				//update data structure(Time Stamps , content lenght , content hash)
			}
			else{
				exception = new SystemException();
				exception.setMessage("You cant overwrite a file of different owner. Enter valid arguments");
				throw exception;
			}
		}
		else{
			//create a new file
			
			try {
				newFile = new File("./output/"+absoluteFileName);
				FileOutputStream is = new FileOutputStream(newFile);
			    OutputStreamWriter osw = new OutputStreamWriter(is);
				BufferedWriter writer = new BufferedWriter(osw);
				writer.write(rFile.getContent());
				writer.close();
				is.close();
				osw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			rFile.setContent(contents);
			userMetadata.setCreated(System.currentTimeMillis());
			userMetadata.setUpdated(System.currentTimeMillis());
			userMetadata.setVersion(0);
			userMetadata.setDeleted(0);
			userMetadata.setContentLength(contents.length());
			String shaHash = sha256(contents);
			userMetadata.setContentHash(shaHash);
			rFile.setMeta(userMetadata);
			fileStorage.put(fileName, rFile);
		}
		
	}
	
	@Override
	public RFile readFile(String filename, String owner)
			throws SystemException, TException {
		SystemException exception=null;
		RFile rFile=null;
		String[] name= filename.split("\\.");
		String key = name[0];
		if(!(fileStorage.containsKey(key))){
			exception= new SystemException();
			exception.setMessage("File not found. Please enter valid file name");
			throw exception;
		}
		else{
			rFile = fileStorage.get(key);
			RFileMetadata metadata= rFile.getMeta();
			if(metadata.getOwner().equals(owner)){
				if(metadata.getDeleted()==0){
					return rFile;
				}
				else{
					exception= new SystemException();
					exception.setMessage("This file is deleted");
					throw exception;
				}
				
			}
			else{
				exception= new SystemException();
				exception.setMessage("This file is not owned by the specified owner");
				throw exception;
			}
		}
		
	}

	@Override
	public void deleteFile(String filename, String owner)
			throws SystemException, TException {
		SystemException exception=null;
		RFile rFile;
		String[] name= filename.split("\\.");
		String key = name[0];
		rFile = fileStorage.get(key);
		if(rFile==null){
			exception= new SystemException();
			exception.setMessage("File not found. Please enter valid file name");
			throw exception;
		}
		else{
			RFileMetadata metadata= rFile.getMeta();
			if(metadata.getOwner().equals(owner)){
				if(metadata.getDeleted()==0){
					File file = new File("./output/"+filename);
					if(file.delete()){
						rFile.setContent(null);
						metadata.setDeleted(System.currentTimeMillis());
						rFile.setMeta(metadata);
						fileStorage.put(key, rFile);
					}
				}
				else{
					exception= new SystemException();
					exception.setMessage("This file is already deleted");
					throw exception;
				}
				
			}
			else{
				exception= new SystemException();
				exception.setMessage("This file is not owned by the specified owner");
				throw exception;
			}
		}

		
	}

	@Override
	public void setFingertable(List<NodeID> node_list) throws TException {
		nodeList = node_list;
		for(NodeID node: node_list){
			fingerTable.put(node.getId(), node);
		}
	}

	@Override
	public NodeID findSucc(String key) throws SystemException, TException {
		
		//check if the current node is the successor of the specified key
		//If yes, return this node else call findPred(key)
		
		
		String currentNodeKey;
		NodeID successorNode=null,predecessorNode=null;
		String currentIp=null;
		try {
			 currentIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		String port = Integer.toString(portNumber);
		currentNodeKey = sha256(currentIp+":"+port);
		
		//check if the current node is the successor of the specified key
		//If yes, return this node else call findPred(key)
		int result=currentNodeKey.compareToIgnoreCase(key);
		if(result==0){
			successorNode = new NodeID();
			successorNode.setId(currentNodeKey);
			successorNode.setIp(currentIp);
			successorNode.setPort(portNumber);
			
		}else{
			try{
				predecessorNode = findPred(key);
				if(predecessorNode==null){
					SystemException nullException = new SystemException();
					nullException.setMessage("Node cannot be found");
					throw nullException;
				}
			}catch(SystemException exception){
				throw exception;
			}
			
			if(predecessorNode!=null){
				String host = predecessorNode.getIp();
				int pPort = predecessorNode.getPort();
				if(pPort==portNumber){
					successorNode=this.getNodeSucc();
				}
				else{
					try {
						transport  = new TSocket(host,pPort);
						transport.open();
						protocol = new TBinaryProtocol(transport);
						FileStore.Client client = new FileStore.Client(protocol);
						try {
							successorNode=client.getNodeSucc();
						} catch (SystemException e) {
							throw e;
						} catch (TException e) {
							e.printStackTrace();
							System.exit(0);
						}
						transport.close();
					} catch (TTransportException e) {
						e.printStackTrace();
						System.exit(0);
					}
				}	
			}
		}		 
		return successorNode;
	}

	@Override
	public NodeID findPred(String key) throws SystemException, TException {
		//key.compareToIgnoreCase(currentKey);
		//negative value if specified key is greater;
		//zero if specified key is equal;
		//positive value if specified key is smaller;
		
		NodeID predNode=null,tempNode,currentNode,targetNode;
		String currentNodeKey;
		String currentIp=null;
		try {
			currentIp = InetAddress.getLocalHost().getHostAddress();
			
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		String port = Integer.toString(portNumber);
		currentNodeKey = sha256(currentIp+":"+port);
		
		
		String secondKey;
		int result,value,tempResult,lessValue,greaterValue;
		
		//Create current node object
		currentNode = new NodeID();
		currentNode.setId(currentNodeKey);
		currentNode.setIp(currentIp);
		currentNode.setPort(portNumber);
		//Create current node object
		
		//Check if key lies between currentNode and First entry in fingerTable
		//If satisfied, currentNode is the predecessor
		
		if(nodeList.size()>0){
			tempNode = nodeList.get(0);
			secondKey = tempNode.getId();
			//Check if second key is smaller than the first key to adjust order of comparison
			result=currentNodeKey.compareToIgnoreCase(secondKey);
			if(result>0){
				value=key.compareToIgnoreCase(secondKey);
				lessValue=key.compareToIgnoreCase(secondKey);
				greaterValue=key.compareToIgnoreCase(currentNodeKey);
				if(lessValue<=0||greaterValue>=0){
					predNode=currentNode;
					//return predNode; 
				}
				else{
					targetNode = compareNodeList(key);
					try{
						predNode = nextRpcCall(targetNode,key);
					}catch(SystemException exception){
						throw exception;
					}
				}
			}
			else{
				value=key.compareToIgnoreCase(currentNodeKey);
				if(value>0){
					tempResult = key.compareToIgnoreCase(secondKey);
					if(tempResult<=0){
						predNode = currentNode;
					}
					else{
						targetNode = compareNodeList(key);
						try{
							predNode = nextRpcCall(targetNode,key);
						}catch(SystemException exception){
							throw exception;
						}
						//RPC on targetNode
					}
					
				}
				else{
					targetNode = compareNodeList(key);
					try{
						predNode = nextRpcCall(targetNode,key);
					}catch(SystemException exception){
						throw exception;
					}
					//RPC on targetNode
				}
			}
		}
		else{
			return null;
		}
		return predNode;
	}

	private NodeID nextRpcCall(NodeID targetNode, String key) throws SystemException,TException{
		
		NodeID predNode=null;
		
		if(portNumber == targetNode.getPort()){
			try {
				SystemException  exception = new SystemException();
				exception.setMessage("Calling same port again, Last entry is same as current node.Infinit loop");			
				throw exception;
			} catch (SystemException e) {
				
				e.printStackTrace();
			}
		}
		String host = targetNode.getIp();
		int port = targetNode.getPort();
		
		try {
			transport  = new TSocket(host,port);
			transport.open();
			protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			try {
				predNode=client.findPred(key);
			} catch (SystemException e) {
				e.printStackTrace();
			} catch (TException e) {
				e.printStackTrace();
			}
			transport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return predNode;
	}
	private NodeID compareNodeList(String key) {
		
		//key.compareToIgnoreCase(currentKey);
		//negative value if specified key is greater;
		//zero if specified key is equal;
		//positive value if specified key is smaller;
				
		
		/*String currentNodeKey;
		String currentIp="192.168.25.145";
		String port = Integer.toString(portNumber);
		currentNodeKey = sha256(currentIp+":"+port);*/
		
		NodeID firstNode,secondNode,targetNode=null;
		String firstKey,secondKey;
		int result,value,tempResult,lessValue,greaterValue;
		int firstCounter=0;
		int secondCounter=1;
		int flag=1;
		while(flag==1){
			firstNode = nodeList.get(firstCounter);
			firstKey = firstNode.getId();
			secondNode = nodeList.get(secondCounter);
			secondKey = secondNode.getId();
			
			//Check if second key is smaller than the first key to adjust order of comparison
			result=firstKey.compareToIgnoreCase(secondKey);
			if(result>0){
				value=key.compareToIgnoreCase(secondKey);
				lessValue=key.compareToIgnoreCase(secondKey);
				greaterValue=key.compareToIgnoreCase(firstKey);
				if(lessValue<=0||greaterValue>=0){
					targetNode = firstNode;
					break;
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=nodeList.size()){
						flag=0;
						targetNode = nodeList.get(nodeList.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			else{
				value=key.compareToIgnoreCase(firstKey);
				if(value>0){
					tempResult = key.compareToIgnoreCase(secondKey);
					if(tempResult<=0){
						targetNode = firstNode;
						//return firstNode;
						break;
					}
					else{
						firstCounter++;
						secondCounter++;
						if(secondCounter>=nodeList.size()){
							flag=0;
							targetNode = nodeList.get(nodeList.size()-1);
							
							break;
						}
					}
				}
				else{
					firstCounter++;
					secondCounter++;
					if(secondCounter>=nodeList.size()){
						flag=0;
						targetNode = nodeList.get(nodeList.size()-1);
						//return targetNode;
						break;
					}
				}
			}
			
		}
		return targetNode;
	}
	
	@Override
	public NodeID getNodeSucc() throws SystemException, TException {
		
		NodeID succNode = null;
		if(nodeList.size()>0){
			succNode = nodeList.get(0);
		}
		
		return succNode;
	}
	
	public static String sha256(String base) {
	    try{
	        MessageDigest digest = MessageDigest.getInstance("SHA-256");
	        byte[] hash = digest.digest(base.getBytes("UTF-8"));
	        StringBuffer hexString = new StringBuffer();

	        for (int i = 0; i < hash.length; i++) {
	            String hex = Integer.toHexString(0xff & hash[i]);
	            if(hex.length() == 1) hexString.append('0');
	            hexString.append(hex);
	        }

	        return hexString.toString();
	    } catch(Exception ex){
	       throw new RuntimeException(ex);
	    }
	}


}
