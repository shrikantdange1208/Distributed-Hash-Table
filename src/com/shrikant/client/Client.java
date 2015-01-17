package com.shrikant.client;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.MessageDigest;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.service.FileStore;
import com.shrikant.service.NodeID;
import com.shrikant.service.RFile;
import com.shrikant.service.RFileMetadata;
import com.shrikant.service.SystemException;

public class Client {
	
	public static void main(String args[]){
		
		TIOStreamTransport jsonTransport = new TIOStreamTransport(System.out);
		TProtocol jsonProtocol = new TJSONProtocol(jsonTransport);
		SystemException sysException;
		
		String host=null;
		int port=0;
		String operation=null;
		String user=null;
		String absoluteFileName=null;
		
		try{
			if(!((args.length==6 || args.length==8))){
				sysException = new SystemException();
				String message = "Enter valid 5 arguments in format:";
				String argumentFormat = "localhost 9090 --opertaion read --fileName example.txt --user guest";
				sysException.setMessage(message+argumentFormat);
				throw sysException;
			}
		}catch(SystemException exception){
			try {
				exception.write(jsonProtocol);
				System.exit(0);
			} catch (TException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		
		host = args[0].trim();
		port = Integer.parseInt(args[1].trim());
		String fieldName;
		for(int i=2; i<args.length; i=i+2){
			
			fieldName = args[i].trim();
			if(fieldName.equals("--operation")){
				operation = args[i+1].trim();
			}
			else if(fieldName.equals("--filename")){
				absoluteFileName = args[i+1].trim();
			}
			else if(fieldName.equals("--user")){
				user = args[i+1].trim();
			}
		}
		
		TTransport transport;		 
		 try {
			transport  = new TSocket(host,port);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			FileStore.Client client = new FileStore.Client(protocol);
			doOperation(client,operation,absoluteFileName,user,transport);
			
		} catch (TTransportException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static void doOperation(FileStore.Client client, String operation, String absoluteFileName,
			String user, TTransport originalTransport) {
		
		TTransport newTransport;
		TProtocol newProtocol;
		
		String key;
		
		NodeID succNode=null;
		int portNumber;
		String ipAddr;
		String fileName=null;
		String[] fileParts = null;
		fileParts = absoluteFileName.split("/");
		fileName = fileParts[fileParts.length-1];
		
		
		RFile rFile = null;
		RFileMetadata metadata = null;
		String contents = null;
		
		TIOStreamTransport jsonTransport = new TIOStreamTransport(System.out);
		TProtocol jsonProtocol = new TJSONProtocol(jsonTransport);
		
		if(operation.equals("write")){
			
			key = sha256(user+":"+fileName);
			
			try {
				
				succNode=client.findSucc(key);
				originalTransport.close();
				
				if(succNode!=null){
					portNumber = succNode.getPort();
					ipAddr = succNode.getIp();
					newTransport = new TSocket(ipAddr,portNumber);
					newTransport.open();
					newProtocol = new TBinaryProtocol(newTransport);
					client = new FileStore.Client(newProtocol);
					
					rFile = new RFile();
					metadata = new RFileMetadata();
					contents = readLocalFile(absoluteFileName);
					if(contents.equals("")){
						System.out.println("File does not exists in local directory");
						System.exit(0);
					}
					rFile.setContent(contents);
					metadata.setOwner(user);
					metadata.setFilename(fileName);
					rFile.setMeta(metadata);
					client.writeFile(rFile);
					newTransport.close();
				}
				
			} catch (SystemException e) {
				try {
					e.write(jsonProtocol);
				} catch (TException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					System.exit(0);
				}
				System.exit(0);
			} catch (TException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		else if(operation.equals("read")){
			key = sha256(user+":"+fileName);
			
			try {
				succNode=client.findSucc(key);
				originalTransport.close();
				if(succNode!=null){
					portNumber = succNode.getPort();
					ipAddr = succNode.getIp();
					newTransport = new TSocket(ipAddr,portNumber);
					newTransport.open();
					newProtocol = new TBinaryProtocol(newTransport);
					client = new FileStore.Client(newProtocol);
					
					rFile=client.readFile(fileName, user);
					rFile.write(jsonProtocol);
					newTransport.close();
				}
				
			} catch (SystemException e) {
				try {
					e.write(jsonProtocol);
					System.exit(0);
				} catch (TException e1) {
					e1.printStackTrace();
					System.exit(0);
				}
			} catch (TException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
		else if(operation.equals("delete")){
			key = sha256(user+":"+fileName);
			
			try {
				
				succNode=client.findSucc(key);
				originalTransport.close();
				if(succNode!=null){
					portNumber = succNode.getPort();
					ipAddr = succNode.getIp();
					
					newTransport = new TSocket(ipAddr,portNumber);
					newTransport.open();
					newProtocol = new TBinaryProtocol(newTransport);
					client = new FileStore.Client(newProtocol);
					
					client.deleteFile(fileName, user);
					newTransport.close();
				}
				
			} catch (SystemException e) {
				try {
					e.write(jsonProtocol);
					System.exit(0);
				} catch (TException e1) {
					e1.printStackTrace();
					System.exit(0);
				}
			} catch (TException e) {
				e.printStackTrace();
				System.exit(0);
			}
		}
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
	
	private static String readLocalFile(String fileName){
		String contents="";
		String line=null;
		BufferedReader br;
		try{
			br = new BufferedReader(new FileReader(fileName));
			while((line = br.readLine())!=null){
				contents+= line+"\n";
			}
			br.close();	
		}catch (FileNotFoundException e) {
				e.printStackTrace();
		}catch (IOException e) {
				e.printStackTrace();
		}
		return contents;
	}
	
}
