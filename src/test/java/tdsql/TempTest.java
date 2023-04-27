package tdsql;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Objects;

@Disabled
public class TempTest {
    @Test
    public void testNullEqual() {
        Integer id = null;

        if (Objects.equals(id, Integer.valueOf(1))) {
            System.out.println("true");
        }
        if (id < 0) {
            System.out.println("false");
        }
    }
}
