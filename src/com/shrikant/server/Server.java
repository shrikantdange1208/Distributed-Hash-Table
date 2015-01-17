package com.shrikant.server;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;

import com.shrikant.handler.FileHandler;
import com.shrikant.service.FileStore;
import com.shrikant.service.FileStore.Iface;

public class Server {
	
	private static FileHandler fileHandler;
	private static FileStore.Processor<Iface> processor;
	
	public static void main(String args[]){
		
		if(args.length!=1){
			System.out.println("Please enter port number");
			System.exit(0);
		}
		int portNumber = Integer.parseInt(args[0].trim());
		fileHandler = new FileHandler(portNumber);
		processor = new FileStore.Processor<FileStore.Iface>(fileHandler);
		startServer(processor,portNumber);
		
		
	}

	private static void startServer(FileStore.Processor<Iface> processor,int portNumber) {
		try {
			
			TServerTransport serverTransport = new TServerSocket(portNumber);
			TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
			System.out.println("Staring the file server");
			server.serve();
			serverTransport.close();
		} catch (TTransportException e) {
			e.printStackTrace();
		}
		
	}
}
