#include <storage.h>
#include <policies.h>


using namespace std;


template < typename Policy > void performance_test()
{
    using Storage = ::jb::Storage< Policy >;
    using RetCode = typename Storage::RetCode;
    using KeyValue = typename Storage::KeyValue;
    using Value = typename Storage::Value;

    auto cleanup = []{
        for ( auto & p : filesystem::directory_iterator( "." ) )
        {
            if ( p.is_regular_file() && p.path().extension() == ".jb" )
            {
                filesystem::remove( p.path() );
            }
        }
    };

    cleanup();

    auto[ rc, pv ] = Storage::OpenPhysicalVolume( "Performance.jb" );
    assert( RetCode::Ok == rc );

    auto[ rc1, vv ] = Storage::OpenVirtualVolume();
    assert( RetCode::Ok == rc );

    auto[ rc3, mp ] = vv.Mount( pv, "/", "/", "mount0" );

    static constexpr size_t TestLimit = 25000;

    std::cout << std::endl << "Inserting " << TestLimit << " keys..." << std::endl;

    uint64_t insertion_total_time = 0;
    for ( size_t i = 0; i < TestLimit; ++i )
    {
        KeyValue key = "key_" + to_string( i );
        Value value{ i };

        const uint64_t start = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );
        vv.Insert( "/mount0", key, move( value ) );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        insertion_total_time += end - start;
    }

    std::cout << "Done: average insertion time: " << insertion_total_time / TestLimit << " microseconds" << std::endl;

    //----------------------------------------------------------------------------------------------------------------------

    std::cout << std::endl << "Getting " << TestLimit << " keys..." << std::endl;

    uint64_t getting_total_time = 0;
    for ( size_t i = 0; i < TestLimit; ++i )
    {
        KeyValue key = "/mount0/key_" + to_string( i );

        const uint64_t start = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );
        auto [rc, v] = vv.Get( key );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        getting_total_time += end - start;
    }

    std::cout << "Done: average getting time: " << getting_total_time / TestLimit << " microseconds" << std::endl;

    //----------------------------------------------------------------------------------------------------------------------

    std::cout << std::endl << "Getting " << TestLimit/10 << " invalid keys..." << std::endl;

    uint64_t getting_i_total_time = 0;
    for ( size_t i = 0; i < TestLimit/10; ++i )
    {
        KeyValue key = "/mount0/ikey_" + to_string( i );

        const uint64_t start = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );
        auto[ rc, v ] = vv.Get( key );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        getting_i_total_time += end - start;
    }

    std::cout << "Done: average reject time: " << getting_i_total_time / TestLimit << " microseconds" << std::endl;

    Storage::CloseAll();

    cleanup();
}


int main( int argc, char **argv )
{
    //-----------------------------------------------
    performance_test< jb::DefaultPolicy<> >();
    //-----------------------------------------------
    return 0;
}
