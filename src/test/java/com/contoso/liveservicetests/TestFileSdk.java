package com.contoso.liveservicetests;

public class TestFileSdk {

    /*
    Create tests:
    1. Create small file (~1K or less) and read contents to verify
    2. Create file, write small text to it thousands of times, and read contents to verify.
    3. Create file, write large byte buffer (11 MB)
    4. Create file, do two 4MB writes and 1 1MB write and then read and verify.
    5. Create file, write 5 2MB writes and then read and verify
    6. Create file, then create another file with overwrite, and then read to ensure you get new content
    7. Create file, then create another file with no overwrite and ensure the create fails
    */

}
