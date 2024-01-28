
#include "eckit/testing/Test.h"

#include "fdb5/database/CartesianProduct.h"

using namespace std::string_literals;

//----------------------------------------------------------------------------------------------------------------------

CASE("Test output ordering") {
    std::vector<int> output(3);

    std::vector<int> v1 {1, 2, 3};
    std::vector<int> v2 {4, 5, 6};
    std::vector<int> v3 {7, 8, 9};

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(v1, output[0]);
    cp.append(v2, output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}, {1, 8, 4}, {1, 9, 4},
        {1, 7, 5}, {1, 8, 5}, {1, 9, 5},
        {1, 7, 6}, {1, 8, 6}, {1, 9, 6},
        {2, 7, 4}, {2, 8, 4}, {2, 9, 4},
        {2, 7, 5}, {2, 8, 5}, {2, 9, 5},
        {2, 7, 6}, {2, 8, 6}, {2, 9, 6},
        {3, 7, 4}, {3, 8, 4}, {3, 9, 4},
        {3, 7, 5}, {3, 8, 5}, {3, 9, 5},
        {3, 7, 6}, {3, 8, 6}, {3, 9, 6}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Test unit-sized vectors") {
    std::vector<int> output(3);

    std::vector<int> v1 {1, 2, 3};
    std::vector<int> v2 {4};
    std::vector<int> v3 {7, 8, 9};

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(v1, output[0]);
    cp.append(v2, output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}, {1, 8, 4}, {1, 9, 4},
        {2, 7, 4}, {2, 8, 4}, {2, 9, 4},
        {3, 7, 4}, {3, 8, 4}, {3, 9, 4},
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Test scalar elements") {
    std::vector<int> output(3);

    std::vector<int> v1 {1, 2, 3};
    std::vector<int> v3 {7, 8, 9};

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(v1, output[0]);
    cp.append(int(4), output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}, {1, 8, 4}, {1, 9, 4},
        {2, 7, 4}, {2, 8, 4}, {2, 9, 4},
        {3, 7, 4}, {3, 8, 4}, {3, 9, 4},
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Test all unit-sized vectors") {
    std::vector<int> output(3);

    std::vector<int> v1 {1};
    std::vector<int> v2 {4};
    std::vector<int> v3 {7};

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(v1, output[0]);
    cp.append(v2, output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Test all scalar elements") {
    std::vector<int> output(3);

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(int(1), output[0]);
    cp.append(int(4), output[2]);
    cp.append(int(7), output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Empty lists are invalid") {
    std::vector<int> output(3);

    std::vector<int> v1 {1, 2, 3};
    std::vector<int> v2 {};
    std::vector<int> v3 {7, 8, 9};

    fdb5::CartesianProduct<std::vector<int>> cp;

    cp.append(v1, output[0]);
    EXPECT_THROWS_AS(cp.append(v2, output[2]), eckit::BadValue);
}

CASE("Entries are mandatory") {
    fdb5::CartesianProduct<std::vector<int>> cp;
    EXPECT_THROWS_AS(cp.next(), eckit::SeriousBug);
}

CASE("Testing a non-iterable cartesian product") {
    std::vector<int> output(3);

    fdb5::CartesianProduct<int> cp;

    cp.append(int(1), output[0]);
    cp.append(int(4), output[2]);
    cp.append(int(7), output[1]);

    std::vector<std::vector<int>> expected_output {
        {1, 7, 4}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Works with strings") {
    std::vector<std::string> output(3);

    std::vector<std::string> v1 {"aa", "bb", "cc"};
    std::vector<std::string> v3 {"dd", "ee", "ff"};

    fdb5::CartesianProduct<std::vector<std::string>> cp;

    cp.append(v1, output[0]);
    cp.append("gg", output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<std::string>> expected_output {
        {"aa", "dd", "gg"}, {"aa", "ee", "gg"}, {"aa", "ff", "gg"},
        {"bb", "dd", "gg"}, {"bb", "ee", "gg"}, {"bb", "ff", "gg"},
        {"cc", "dd", "gg"}, {"cc", "ee", "gg"}, {"cc", "ff", "gg"},
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}


CASE("Works with c-style strings") {
    std::vector<const char*> output(3);

    std::vector<const char*> v1 {"aa", "bb", "cc"};
    std::vector<const char*> v3 {"dd", "ee", "ff"};

    fdb5::CartesianProduct<std::vector<const char*>> cp;

    cp.append(v1, output[0]);
    cp.append("gg", output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<std::string>> expected_output {
        {"aa", "dd", "gg"}, {"aa", "ee", "gg"}, {"aa", "ff", "gg"},
        {"bb", "dd", "gg"}, {"bb", "ee", "gg"}, {"bb", "ff", "gg"},
        {"cc", "dd", "gg"}, {"cc", "ee", "gg"}, {"cc", "ff", "gg"},
    };

    int idx = 0;
    while (cp.next()) {
    for (int i = 0; i < output.size(); ++i) EXPECT(output[i] == expected_output[idx][i]);
        ++idx;
    }
    idx = 0;
    while (cp.next()) {
    for (int i = 0; i < output.size(); ++i) EXPECT(output[i] == expected_output[idx][i]);
        ++idx;
    }
    }


CASE("Works with string_viewss") {
    std::vector<std::string_view> output(3);

    std::vector<std::string> v1 {"aa", "bb", "cc"};
    std::string v2("gg");
    std::vector<std::string> v3 {"dd", "ee", "ff"};

    fdb5::CartesianProduct<std::vector<std::string>, std::string_view> cp;

    cp.append(v1, output[0]);
    cp.append(v2, output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<std::string_view>> expected_output {
        {"aa", "dd", "gg"}, {"aa", "ee", "gg"}, {"aa", "ff", "gg"},
        {"bb", "dd", "gg"}, {"bb", "ee", "gg"}, {"bb", "ff", "gg"},
        {"cc", "dd", "gg"}, {"cc", "ee", "gg"}, {"cc", "ff", "gg"},
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Works with non-iterable string types") {
    std::vector<std::string> output(3);

    fdb5::CartesianProduct<std::string> cp;

    cp.append("testing"s, output[0]);
    cp.append("gnitset"s, output[2]);
    cp.append("again another string"s, output[1]);

    std::vector<std::vector<std::string>> expected_output {
        {"testing", "again another string", "gnitset"}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

CASE("Works with non-iterable string types") {
    std::vector<std::string_view> output(3);

    fdb5::CartesianProduct<const char*, std::string_view> cp;

    cp.append("testing", output[0]);
    cp.append("gnitset", output[2]);
    cp.append("again another string", output[1]);

    std::vector<std::vector<std::string>> expected_output {
        {"testing", "again another string", "gnitset"}
    };

    int idx = 0;
    while (cp.next()) {
    for (int i = 0; i < output.size(); ++i) EXPECT(output[i] == expected_output[idx][i]);
        ++idx;
    }
    idx = 0;
    while (cp.next()) {
    for (int i = 0; i < output.size(); ++i) EXPECT(output[i] == expected_output[idx][i]);
        ++idx;
    }
}

CASE("Works with non-iterable string_view types") {
    std::vector<std::string_view> output(3);

    fdb5::CartesianProduct<std::string, std::string_view> cp;

    std::string v1("testing");
    std::string v2("gnitset");
    std::string v3("again another string");

    cp.append(v1, output[0]);
    cp.append(v2, output[2]);
    cp.append(v3, output[1]);

    std::vector<std::vector<std::string_view>> expected_output {
        {"testing", "again another string", "gnitset"}
    };

    int idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
    idx = 0;
    while (cp.next()) {
        EXPECT(output == expected_output[idx++]);
    }
}

//----------------------------------------------------------------------------------------------------------------------

int main(int argc, char** argv) {
    return ::eckit::testing::run_tests(argc, argv);
}
