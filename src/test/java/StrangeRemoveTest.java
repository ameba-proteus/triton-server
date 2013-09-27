

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class StrangeRemoveTest {
	
	private Cluster cluster;
	
	private Session session;

	public StrangeRemoveTest() {
	}
	
	@Before
	public void before() {
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();
		session.execute("create keyspace strange_remove_test with replication={'class':'SimpleStrategy', 'replication_factor': 1}");
		session.shutdown();
		session = cluster.connect("strange_remove_test");
		session.execute("create table sample (id text, column text, value text, primary key(id, column))");
	}
	
	@After
	public void after() {
		session.execute("drop keyspace strange_remove_test");
		session.shutdown();
	}

	@Test
	public void testStrangeGet() throws Exception {
		
		PreparedStatement ps = session.prepare("insert into sample (id, column, value) values (?, ?, ?)");
		session.execute(ps.bind("id1", "column1", "value11"));
		session.execute(ps.bind("id2", "column1", "value21"));
		session.execute(ps.bind("id3", "column1", "value31"));
		session.execute(ps.bind("id3", "column2", "value32"));
		
		ResultSet rs = session.execute("select id, column, value from sample");
		Assert.assertEquals(4, rs.all().size());
		
		session.execute(session.prepare("delete from sample where id = ?").bind("id3"));

		rs = session.execute("select id, column, value from sample");
		Assert.assertEquals(2, rs.all().size());

	}
	
}

