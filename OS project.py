import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

class File:
    def __init__(self, name: str, content: str, size: int):
        self.name = name
        self.content = content
        self.size = size
        self.created_at = time.time()
        self.modified_at = time.time()
        self.chunks = self._split_into_chunks()
        
    def _split_into_chunks(self, chunk_size: int = 64) -> List[str]:
        """Split file content into fixed-size chunks (simulating GFS/HDFS)"""
        return [self.content[i:i+chunk_size] for i in range(0, len(self.content), chunk_size)]
    
    def update_content(self, new_content: str):
        self.content = new_content
        self.modified_at = time.time()
        self.chunks = self._split_into_chunks()

@dataclass
class Node:
    id: str
    storage: Dict[str, File] = None  # Files stored on this node
    cache: Dict[str, File] = None    # Cached files (simulating AFS)
    
    def __post_init__(self):
        self.storage = {}
        self.cache = {}

class DistributedFileSystem(ABC):
    @abstractmethod
    def store_file(self, file: File) -> bool:
        pass
    
    @abstractmethod
    def get_file(self, filename: str) -> Optional[File]:
        pass
    
    @abstractmethod
    def replicate_file(self, filename: str) -> bool:
        pass

class HDFS(DistributedFileSystem):
    def __init__(self):
        self.name_node = Node(id="name_node")  # Master node
        self.data_nodes = [Node(id=f"data_node_{i}") for i in range(3)]  # Slave nodes
        self.replication_factor = 3
    
    def store_file(self, file: File) -> bool:
        """Store file with replication across data nodes"""
        if file.name in self.name_node.storage:
            return False
        
        # Store metadata in name node
        self.name_node.storage[file.name] = file
        
        # Distribute chunks to data nodes with replication
        for chunk_idx, chunk in enumerate(file.chunks):
            for i in range(self.replication_factor):
                node_idx = (chunk_idx + i) % len(self.data_nodes)
                chunk_name = f"{file.name}_chunk_{chunk_idx}"
                self.data_nodes[node_idx].storage[chunk_name] = File(chunk_name, chunk, len(chunk))
        
        return True
    
    def get_file(self, filename: str) -> Optional[File]:
        """Retrieve file by assembling chunks from data nodes"""
        if filename not in self.name_node.storage:
            return None
            
        # Get metadata from name node
        file = self.name_node.storage[filename]
        
        # Reconstruct file from chunks
        reconstructed_content = []
        for chunk_idx in range(len(file.chunks)):
            chunk_name = f"{filename}_chunk_{chunk_idx}"
            
            # Try to get chunk from any node that has it
            for node in self.data_nodes:
                if chunk_name in node.storage:
                    reconstructed_content.append(node.storage[chunk_name].content)
                    break
        
        file.content = "".join(reconstructed_content)
        return file
    
    def replicate_file(self, filename: str) -> bool:
        """Ensure all chunks meet replication factor"""
        # Implementation would check replication status and create additional copies if needed
        return True

class AFS(DistributedFileSystem):
    def __init__(self):
        self.servers = [Node(id=f"server_{i}") for i in range(2)]
        self.clients = [Node(id=f"client_{i}") for i in range(3)]
        self.authentication_server = Node(id="auth_server")
        
    def store_file(self, file: File) -> bool:
        """Store file on server with authentication"""
        # Simulate authentication
        if not self._authenticate():
            return False
            
        # Store on first available server
        self.servers[0].storage[file.name] = file
        return True
    
    def get_file(self, filename: str) -> Optional[File]:
        """Get file with client-side caching"""
        # Check client cache first (simulating AFS caching)
        for client in self.clients:
            if filename in client.cache:
                return client.cache[filename]
        
        # If not in cache, get from server
        for server in self.servers:
            if filename in server.storage:
                file = server.storage[filename]
                # Cache on first client
                self.clients[0].cache[filename] = file
                return file
                
        return None
    
    def replicate_file(self, filename: str) -> bool:
        """AFS replication across servers"""
        for server in self.servers:
            if filename in server.storage:
                # Copy to other servers
                for other_server in self.servers:
                    if other_server != server:
                        other_server.storage[filename] = server.storage[filename]
                return True
        return False
    
    def _authenticate(self) -> bool:
        """Simulate Kerberos authentication"""
        return True  # Simplified for demo

class GFS(DistributedFileSystem):
    def __init__(self):
        self.master = Node(id="master")
        self.chunk_servers = [Node(id=f"chunk_server_{i}") for i in range(4)]
        self.chunk_size = 64  # Fixed chunk size
    
    def store_file(self, file: File) -> bool:
        """Store file in chunks with master coordination"""
        if file.name in self.master.storage:
            return False
            
        # Store metadata in master
        self.master.storage[file.name] = file
        
        # Distribute chunks to chunk servers
        for chunk_idx, chunk in enumerate(file.chunks):
            # Choose 3 servers for each chunk (replication)
            for i in range(3):
                server_idx = (chunk_idx + i) % len(self.chunk_servers)
                chunk_name = f"{file.name}_chunk_{chunk_idx}"
                self.chunk_servers[server_idx].storage[chunk_name] = File(chunk_name, chunk, len(chunk))
        
        return True
    
    def get_file(self, filename: str) -> Optional[File]:
        """Get file by assembling chunks"""
        if filename not in self.master.storage:
            return None
            
        file = self.master.storage[filename]
        
        # Reconstruct from chunks
        content = []
        for chunk_idx in range(len(file.chunks)):
            chunk_name = f"{filename}_chunk_{chunk_idx}"
            
            # Get from nearest chunk server (simplified)
            for server in self.chunk_servers:
                if chunk_name in server.storage:
                    content.append(server.storage[chunk_name].content)
                    break
        
        file.content = "".join(content)
        return file
    
    def replicate_file(self, filename: str) -> bool:
        """Ensure chunk replication is maintained"""
        # Implementation would check and fix under-replicated chunks
        return True

# Demonstration of the systems
if __name__ == "__main__":
    print("=== Distributed File Systems Comparative Demo ===")
    
    # Create a sample file
    sample_file = File("research_paper.txt", "This is a sample research paper content for testing distributed file systems." * 10, 1000)
    
    # Test HDFS
    print("\nTesting Hadoop Distributed File System (HDFS):")
    hdfs = HDFS()
    hdfs.store_file(sample_file)
    retrieved_file = hdfs.get_file("research_paper.txt")
    print(f"Retrieved file content (first 100 chars): {retrieved_file.content[:100]}...")
    print(f"File split into {len(retrieved_file.chunks)} chunks")
    
    # Test AFS
    print("\nTesting Andrew File System (AFS):")
    afs = AFS()
    afs.store_file(sample_file)
    retrieved_file = afs.get_file("research_paper.txt")  # Should be cached after first retrieval
    print(f"Retrieved file content (first 100 chars): {retrieved_file.content[:100]}...")
    print("File cached on client:", "research_paper.txt" in afs.clients[0].cache)
    
    # Test GFS
    print("\nTesting Google File System (GFS):")
    gfs = GFS()
    gfs.store_file(sample_file)
    retrieved_file = gfs.get_file("research_paper.txt")
    print(f"Retrieved file content (first 100 chars): {retrieved_file.content[:100]}...")
    print(f"File split into {len(retrieved_file.chunks)} chunks of size {gfs.chunk_size}")