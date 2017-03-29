/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.datanode.nvmf;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.datanode.nvmf.client.NvmfStorageEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.disni.nvmef.NvmeEndpointGroup;
import com.ibm.disni.nvmef.NvmeServerEndpoint;
import com.ibm.disni.nvmef.spdk.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;

public class NvmfStorageTier extends StorageTier {

	private static final Logger LOG = CrailUtils.getLogger();
	private InetSocketAddress datanodeAddr;
	private NvmeEndpointGroup clientGroup;

	public InetSocketAddress getAddress() {
		if (datanodeAddr == null) {
			datanodeAddr = new InetSocketAddress(NvmfStorageConstants.IP_ADDR, NvmfStorageConstants.PORT);
		}
		return datanodeAddr;
	}

	public void printConf(Logger logger) {
		NvmfStorageConstants.printConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		NvmfStorageConstants.updateConstants(crailConfiguration);

		if (args != null) {
			Options options = new Options();
			Option bindIp = Option.builder("a").desc("ip address to bind to").hasArg().build();
			Option port = Option.builder("p").desc("port to bind to").hasArg().type(Number.class).build();
			Option pcieAddress = Option.builder("s").desc("PCIe address of NVMe device").hasArg().build();
			options.addOption(bindIp);
			options.addOption(port);
			options.addOption(pcieAddress);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine line = null;
			try {
				line = parser.parse(options, Arrays.copyOfRange(args, 2, args.length));
				if (line.hasOption(port.getOpt())) {
					NvmfStorageConstants.PORT = ((Number) line.getParsedOptionValue(port.getOpt())).intValue();
				}
			} catch (ParseException e) {
				System.err.println(e.getMessage());
				formatter.printHelp("NVMe storage tier", options);
				System.exit(-1);
			}
			if (line.hasOption(bindIp.getOpt())) {
				NvmfStorageConstants.IP_ADDR = InetAddress.getByName(line.getOptionValue(bindIp.getOpt()));
			}
			if (line.hasOption(pcieAddress.getOpt())) {
				NvmfStorageConstants.PCIE_ADDR = line.getOptionValue(pcieAddress.getOpt());
			}
		}

		NvmfStorageConstants.verify();
	}

	public StorageEndpoint createEndpoint(InetSocketAddress inetSocketAddress) throws IOException {
		if (clientGroup == null) {
			synchronized (this) {
				if (clientGroup == null) {
					clientGroup = new NvmeEndpointGroup(new NvmeTransportType[]{NvmeTransportType.RDMA},
							NvmfStorageConstants.HUGEDIR,
							NvmfStorageConstants.SOCKETMEM);
				}
			}
		}
		return new NvmfStorageEndpoint(clientGroup, inetSocketAddress);
	}

	public void run() throws Exception {
		LOG.info("initalizing NVMf datanode");

		NvmeEndpointGroup group = new NvmeEndpointGroup(
				new NvmeTransportType[]{NvmeTransportType.PCIE, NvmeTransportType.RDMA}, NvmfStorageConstants.HUGEDIR,
				NvmfStorageConstants.SOCKETMEM);
		NvmeServerEndpoint serverEndpoint = group.createServerEndpoint();
		URI url = new URI("nvmef://" + NvmfStorageConstants.IP_ADDR.getHostAddress() + ":" + NvmfStorageConstants.PORT +
				"/0/1?subsystem=nqn.2016-06.io.spdk:cnode1&pci=" + NvmfStorageConstants.PCIE_ADDR);
		serverEndpoint.bind(url);

		NvmeController controller = serverEndpoint.getNvmecontroller();
		NvmeNamespace namespace = controller.getNamespace(NvmfStorageConstants.NAMESPACE);
		long namespaceSize = namespace.getSize();
		long alignedSize = namespaceSize - (namespaceSize % NvmfStorageConstants.ALLOCATION_SIZE);

		Thread server = new Thread(new NvmfStorageServer(serverEndpoint, getAddress()));
		server.start();

		long addr = 0;
		while (alignedSize > 0) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());

			LOG.info("new block, length " + NvmfStorageConstants.ALLOCATION_SIZE);
			LOG.debug("block stag 0, addr " + addr + ", length " + NvmfStorageConstants.ALLOCATION_SIZE);
			alignedSize -= NvmfStorageConstants.ALLOCATION_SIZE;
			this.setBlock(addr, (int)NvmfStorageConstants.ALLOCATION_SIZE, 0);
			addr += NvmfStorageConstants.ALLOCATION_SIZE;
		}

		while (server.isAlive()) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
			Thread.sleep(2000);
		}

		server.join();
	}

	public void close() throws Exception {

	}
}
