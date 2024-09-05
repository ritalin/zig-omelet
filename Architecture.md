## 起動

* ホストが、全ステージを起動する
* 各ステージが、ホストに、`launched`イベントを発行する
    * 起動できない場合は`failed_launching`イベントを発行する
    * ホストが、いずれかのステージから`failed_launching`を受け取ったら、全ステージに`quit_all`を発行する。
* ホストが、全stageに、`request_topic`イベントを発行する
    * 各ステージが、ホストに、`topic`イベントを通知する
        * topic
    * 各ステージが、ホストに、`end_topic`イベントを発行する

## パイプライン始動

* ホストが、ExtractStageに、`ready_watch_path`イベントを発行する
    * 実際受け取るのは、WatchStageのみ
* FileWatchStageが、ホストに、`source_path` イベントを発行する
    * パスとハッシュ
    * ホストが、パスとハッシュから同期情報を構成する
    * FileWatchStageが、全てのソースを送信したら、`finish_watch_path`イベントを発行する
* ホストが、ExtractStageに`source_path`イベントを発行する
    * パスとハッシュ
* ExtractStageが、ホストに、`topic_body`イベントを発行する
    * topicとpayload + パス/ハッシュ
    * ホストが、同期情報を更新する
        * ソースの変更により同期情報が変更されていれば破棄する
    * 全てのソースを通知し終えたら、`finish_topic_body`イベントを発行する
* ホストが、`topic_body`の全ての`topic`を受け取ったら、GenerateStageに、`ready_topic_body`イベントを発行する
* GenerateStageは`ready_generate`を返答する
* ホストが、GenerateStageに`topic_body`を発行する
    * `source_path`のペイロードも含める
* GenerateStageは、ホストに`ready_generate`を発行する
    * ホストが、全ての`topic_body`を流し切るまで繰り返す
    * 流し切ったら、ホストが、GenerateStageに、`finish_topic_body`イベントを発行する
    * GenerateStageは、すべてのコード生成を終えた際にホストに`finish_generate`を発行する

## Stage

### Runner

omelet

* ステージとの間のイベントのコーディネート

### WatchStage

omelet-watch-files

* ファイルの発行と変更監視
    * (TODO) 変更監視は未実装

#### CLI引数

* `source-dir`
    * ファイルまたはフォルダ内のファイルを`source_path`イベントとしてホストに送信する
* `schema-dir`
    * CLI引数で渡されたファイルまたはフォルダ内のファイルを`source_path`イベントとしてホストに送信する
    * ユーザ定義情報で型を生成する必要があるため
* `exclude-filter`
    * 合致するパスは配信から除外する
* `include-filter`
    * 合致するパスのみを配信する
    * `include-filter`が渡されない場合は全配信
    * `exclude-filter`の方が優先度が高い

### ExtractStage

omelet-duckdb-extract

#### CLI引数

* `schema-dir`
    * 起動時に受け取ったファイルまたはフォルダ内のファイルを実行する

#### `request_topc`受信の際に発行する`toppic`

ソース抽出結果用`topic`

* `query`
    * 名前パラメータを位置パラメータに変更した`SQL`
* `placeholder`
    * パラメータ名と型情報
* `placeholder-order`
    * クエリを実行する際のパラメータの並べ順
    * `typescript`で定義とは異なる順序で初期化しても順序が崩れなくするための措置
* `select-list`
    * マテリアライズされた結果を保持するための型
* `bound-user-type`
    * パラメータ／結果型でユーザ定義型を使用した場合の定義名
* `anon-user-type`
    * ユーザ定義を行わず直接`Enum`等を使用した場合の定義内容
        * 構成は`user-type`と同じ
    * 結果型のフィールドが配列の場合もこのトピックを用いて表現する

ユーザ定義抽出結果用`topic`

* `user-type`
    * (Enum) ユーザ定義名とフィールド名
        * Enumフィールドは型を持たない

#### `source_path`イベントの受信

* `source_path`イベントを受け取ったら、抽出ワーカーをスレッドプールに投げ込む
* 抽出ワーカーとステージは、`Zmq`の`inproc`トランスポートで接続する
* 抽出完了後、抽出ワーカーから`worker_result`を発行する
    * `worker_result`イベントを受け取ったステージは`topic_body`イベントに詰め直してホストに再発行
* 抽出に失敗した場合も同様に、`worker_result`を発行する
    * `worker_result`イベントを受け取ったステージは`skip_topic_body`イベントに詰め直してホストに再発行

#### ユニットテスト

* `C++`側は[catch2](https://github.com/catchorg/Catch2)を使用

### GenerateStage

omelet-ts-generate

* placeholderを置換したSQLを保存
* typescriptのコードを吐く
    * パラメータを渡すための定義
    * マテリアライズされた結果を保持するための定義
    * ユーザ定義型
        * パラメータや結果のフィールド型として使用

#### CLI引数

* `output-dir`
    * 出力先
        * コードの出力先は`$output-dir/${source-dirからの相対パス}`となる
        * ユーザ定義は`$output-dir/user-types/`の直下に作成
            * パラメータや結果型から相対パスで`import`したいがため

## 通信チャネル

* ホスト -> ステージ (Pub/Sub)
* ステージ -> ホスト (Req/Rep)
    * 完了通知/完了受理
    * 終了通知/終了受理 (oneshotのみ)
* ワーカー -> ステージ (Push/Pull)
    * `inproc`トランスポートを使用する
    
## oneshot

`oneshot`で起動した場合。

* ホストが、WatchStageから、`finish_watch_path`イベントを受け取ったら、`quit`イベントを投げ返す
* ホストが、ExtractStageから、`finish_topic_body`イベントを受け取ったら、`quit`イベントを投げ返す
* ホストが、GenerateStageから、`finish_generate`イベントを受け取ったら、`quit`イベントを投げ返す

* `quit`イベントを受け取った各ステージは速やかに`quit_accept`イベントを返信する
* ホストが、全ステージから、`quit_accept`イベントを受け取ったら、イベントループを抜け終了させる

## ログ

* ログレベルは`trace`, `debug`,`info`,`warn`,`error`
: 規定のログレベルは`info`

## 要検討

### ファイルの変更通知

* WatchStageが、非同期で、ファイル変更の通知を受ける
    * WatchStageが、パスとハッシュを評価する
    * WatchStageが、ホストに、source イベントを発行する
* https://github.com/SpartanJ/efsw の使用を検討

### ホストで標準入力ハンドリング

* `q`で終了
    * ホストが、全ステージに、`quit`イベントを発行する
        * ステージが、ホストに、`quit_accept`イベントを発行する
        * ステージを終了させる

### Zmqからnng (NanoMessage-NG)への切り替え

* Zmqよりも多くのトランスポートがサポートされている
    * WebSocketとか
* Zigのバインディングは見つからず
