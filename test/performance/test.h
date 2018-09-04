#ifndef __JB__PERFORMANCE__H__
#define __JB__PERFORMANCE__H__

#include <storage.h>


template < typename Policy > void performance_test()
{
    using Storage = ::jb::Storage< Policy >;
    using RetCode = typename Storage::RetCode;
    using KeyValue = typename Storage::KeyValue;
    using Value = typename Storage::Value;

    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "************************************************************" << std::endl;
    std::cout << std::endl;
    std::cout << "Performance test with the following parameters:" << std::endl;
    std::cout << std::endl;
    std::cout << "B-tree power: " << Policy::PhysicalVolumePolicy::BTreeMinPower << std::endl;
    std::cout << "B-tree cache size: " << Policy::PhysicalVolumePolicy::BTreeCacheSize << " node" << std::endl;
    std::cout << "Storage file chunk size: " << Policy::PhysicalVolumePolicy::ChunkSize << " bytes" << std::endl;
    std::cout << "Bloom filter size: " << Policy::PhysicalVolumePolicy::BloomSize << " bytes" << std::endl;
    std::cout << std::endl;
    std::cout << "************************************************************" << std::endl;

    auto cleanup = [] {
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
        auto[ rc, v ] = vv.Get( key );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        getting_total_time += end - start;
    }

    std::cout << "Done: average getting time: " << getting_total_time / TestLimit << " microseconds" << std::endl;

    //----------------------------------------------------------------------------------------------------------------------

    std::cout << std::endl << "Getting " << TestLimit / 10 << " invalid keys..." << std::endl;

    uint64_t getting_i_total_time = 0;
    for ( size_t i = 0; i < TestLimit / 10; ++i )
    {
        KeyValue key = "/mount0/ikey_" + to_string( i );

        const uint64_t start = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );
        auto[ rc, v ] = vv.Get( key );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        getting_i_total_time += end - start;
    }

    std::cout << "Done: average reject time: " << getting_i_total_time / ( TestLimit / 10 ) << " microseconds" << std::endl;

    //----------------------------------------------------------------------------------------------------------------------

    std::cout << std::endl << "Erasing " << TestLimit / 10 << " keys..." << std::endl;

    uint64_t erasing_total_time = 0;
    for ( size_t i = 0; i < TestLimit / 10; ++i )
    {
        if ( i % 10 ) continue;

        KeyValue key = "/mount0/key_" + to_string( i );

        const uint64_t start = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );
        vv.Erase( key );
        const uint64_t end = std::chrono::system_clock::now().time_since_epoch() / std::chrono::microseconds( 1 );

        erasing_total_time += end - start;
    }

    std::cout << "Done: average erasing time: " << erasing_total_time / ( TestLimit / 10 ) << " microseconds" << std::endl;

    Storage::CloseAll();

    cleanup();
}

#endif