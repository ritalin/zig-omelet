#include <catch2/catch_session.hpp>
#include <iostream>
#include <fstream>

extern "C" {

auto run_catch2_test(const char *redirect_path) -> int {
    std::ofstream out(redirect_path);
    std::streambuf* coutbuf = std::cout.rdbuf(); 
    std::cout.rdbuf(out.rdbuf()); 

    int argc = 1;
    const char* argv[] = { "your_program_name" };

    Catch::Session session;

    session.applyCommandLine(argc, argv);

    auto result = session.run();

    std::cout.rdbuf(coutbuf); 
    return result;
}

}