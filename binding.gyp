{
  'targets': [
    {
      'target_name': 'zongji',
      'sources': [
        'src/zongji.cc'
      ],
      'link_settings': {
        'cflags': [
          '<!@(mysql_config --cflags)'
        ],
        'libraries': [
          '<!@(mysql_config --libs_r)'
        ]
      }
    }
  ],
}
