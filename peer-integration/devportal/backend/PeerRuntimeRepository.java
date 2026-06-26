// Sketch — apply to dev_portal as:
//   backend/src/main/java/io/devportal/peerruntime/PeerRuntimeRepository.java

package io.devportal.peerruntime;

import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PeerRuntimeRepository extends JpaRepository<PeerRuntime, Long> {
    Optional<PeerRuntime> findByName(String name);
    List<PeerRuntime> findByKind(String kind);
    List<PeerRuntime> findByLifecycle(String lifecycle);
}
